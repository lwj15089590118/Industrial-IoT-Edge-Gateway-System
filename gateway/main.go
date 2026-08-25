// ============================================================================
// main.go — 工业物联网边缘网关主程序
// ----------------------------------------------------------------------------
// 职责：
//   1. 作为 Modbus/TCP 客户端，按配置的寄存器地址表轮询读取 10 个保持寄存器；
//   2. 将寄存器原始值按量纲（scale/offset）换算为工程量（电压 V、电流 A 等）；
//   3. 将换算后的数据封装为 JSON，通过 MQTT（QoS1）发布到 Broker；
//   4. 定时采集（默认每秒 1 次），Modbus 与 MQTT 双向断线自动重连；
//   5. 从 config.yaml 加载全部运行参数。
//
// 数据链路：
//   模拟PLC(Modbus Server) --Modbus/TCP--> 本网关 --MQTT--> Broker --> 后端/InfluxDB
// ============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/goburrow/modbus"           // Modbus 协议实现（客户端）
	paho "github.com/eclipse/paho.mqtt.golang" // MQTT 客户端
	"gopkg.in/yaml.v3"                     // YAML 配置解析
)

// ============================================================================
// 一、配置结构定义：与 config.yaml 字段一一对应
// ============================================================================

// ModbusConfig Modbus/TCP 连接参数
type ModbusConfig struct {
	Host     string `yaml:"host"`     // PLC / 模拟器地址
	Port     int    `yaml:"port"`     // Modbus TCP 端口（默认 502）
	UnitID   uint8  `yaml:"unit_id"`  // Modbus 从站号
	Timeout  int    `yaml:"timeout"`  // 单次读写超时（秒）
	SlaveID  uint8  `yaml:"slave_id"` // 兼容字段：部分设备称从站号为 slave id
}

// MQTTConfig MQTT 连接与发布参数
type MQTTConfig struct {
	Broker    string `yaml:"broker"`     // Broker 地址，如 tcp://127.0.0.1:1883
	ClientID  string `yaml:"client_id"`  // 客户端唯一 ID
	Username  string `yaml:"username"`   // 认证用户名（可空）
	Password  string `yaml:"password"`   // 认证密码（可空）
	Topic     string `yaml:"topic"`      // 发布主题，如 factory/line1/metrics
	QoS       byte   `yaml:"qos"`        // 服务质量等级：0/1/2
	RetryWait int    `yaml:"retry_wait"` // 断线重连间隔（秒）
}

// RegisterConfig 单个寄存器的元数据：地址、名称、量纲换算系数
type RegisterConfig struct {
	Address uint16  `yaml:"address"` // 寄存器起始地址（保持寄存器区）
	Quantity uint16 `yaml:"quantity"`// 本测点占用寄存器个数（通常 1）
	Name    string  `yaml:"name"`    // 测点英文名，如 voltage
	Desc    string  `yaml:"desc"`    // 中文描述，如 电压
	Unit    string  `yaml:"unit"`    // 工程单位，如 V
	Scale   float64 `yaml:"scale"`   // 量纲系数：工程量 = 原始值 * scale + offset
	Offset  float64 `yaml:"offset"`  // 零点偏移
}

// GatewayConfig 网关总配置
type GatewayConfig struct {
	Modbus         ModbusConfig     `yaml:"modbus"`
	MQTT           MQTTConfig       `yaml:"mqtt"`
	CollectInterval int             `yaml:"collect_interval_ms"` // 采集周期（毫秒）
	Registers      []RegisterConfig `yaml:"registers"`           // 寄存器地址表
}

// loadConfig 从指定路径读取并解析 YAML 配置文件
func loadConfig(path string) (*GatewayConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	cfg := &GatewayConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	// 配置合法性检查：至少要有一个测点才值得启动
	if len(cfg.Registers) == 0 {
		return nil, fmt.Errorf("配置文件中寄存器地址表为空")
	}
	return cfg, nil
}

// ============================================================================
// 二、采集数据模型：一次轮询得到的全组测点快照
// ============================================================================

// Reading 一次采集的完整快照，JSON 字段即 MQTT 报文体
type Reading struct {
	DeviceID    string             `json:"device_id"`    // 设备标识（网关客户端 ID）
	Timestamp   int64              `json:"timestamp"`    // 采集时间戳（Unix 毫秒）
	Values      map[string]float64 `json:"values"`       // 测点名 -> 工程量
	Units       map[string]string  `json:"units"`        // 测点名 -> 单位（便于前端展示）
	Quality     string             `json:"quality"`      // 数据质量：good / bad
}

// NewReading 构造一个空快照
func NewReading(deviceID string) *Reading {
	return &Reading{
		DeviceID:  deviceID,
		Timestamp: time.Now().UnixMilli(),
		Values:    make(map[string]float64),
		Units:     make(map[string]string),
		Quality:   "good",
	}
}

// ToJSON 序列化为 MQTT 载荷
func (r *Reading) ToJSON() ([]byte, error) {
	return json.Marshal(r)
}

// ============================================================================
// 三、Modbus 客户端封装：连接、按测点逐个读取、指数退避重连
// ============================================================================

// ModbusClient 带自动重连能力的 Modbus/TCP 客户端
type ModbusClient struct {
	cfg    *GatewayConfig
	client modbus.Client
	handler *modbus.TCPClientHandler
	mu     sync.Mutex // 保护连接的建立/重建，避免采集与重连竞争
}

// NewModbusClient 构造未连接的客户端
func NewModbusClient(cfg *GatewayConfig) *ModbusClient {
	return &ModbusClient{cfg: cfg}
}

// Connect 建立到 PLC 的 TCP 连接
func (m *ModbusClient) Connect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	address := fmt.Sprintf("%s:%d", m.cfg.Modbus.Host, m.cfg.Modbus.Port)
	handler := modbus.NewTCPClientHandler(address)
	handler.Timeout = time.Duration(m.cfg.Modbus.Timeout) * time.Second
	handler.SlaveId = m.cfg.Modbus.UnitID // Modbus 单元标识（goburrow/modbus 字段名为 SlaveId）
	if err := handler.Connect(); err != nil {
		return fmt.Errorf("连接 Modbus 设备 %s 失败: %w", address, err)
	}
	m.handler = handler
	m.client = modbus.NewClient(handler)
	log.Printf("[Modbus] 已连接设备 %s (unit_id=%d)", address, m.cfg.Modbus.UnitID)
	return nil
}

// Close 关闭底层连接并置空句柄：
// 置空是关键——EnsureConnected 以 handler==nil 判定断线，
// 若只 Close 不置空，重连逻辑永远不会触发。
func (m *ModbusClient) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handler != nil {
		_ = m.handler.Close()
	}
	m.handler = nil
	m.client = nil
}

// ReadRegisters 读取单个测点对应的保持寄存器，返回原始字节数组
func (m *ModbusClient) ReadRegisters(reg RegisterConfig) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client == nil {
		return nil, fmt.Errorf("Modbus 连接未初始化")
	}
	return m.client.ReadHoldingRegisters(reg.Address, reg.Quantity)
}

// EnsureConnected 检查并按需重建连接（断线重连入口）
func (m *ModbusClient) EnsureConnected() error {
	m.mu.Lock()
	needReconnect := m.handler == nil
	m.mu.Unlock()
	if !needReconnect {
		return nil // 已连接，直接返回
	}
	log.Println("[Modbus] 检测到连接断开，尝试重连...")
	return m.Connect()
}

// ============================================================================
// 四、MQTT 发布器封装：连接保活、断线重连、JSON 发布
// ============================================================================

// MQTTPublisher 带自动重连能力的 MQTT 发布客户端
type MQTTPublisher struct {
	cfg  *MQTTConfig
	cli  paho.Client
	mu   sync.Mutex
}

// NewMQTTPublisher 构造并完成首次连接。
// 注意：首连失败也必须返回可用对象（err 同时返回），
// 否则主流程拿到 nil 后调用 Publish 会空指针崩溃——
// Docker 编排下网关常先于 Broker 就绪，此路径是必现场景。
func NewMQTTPublisher(cfg *MQTTConfig) (*MQTTPublisher, error) {
	p := &MQTTPublisher{cfg: cfg}
	err := p.Connect()
	return p, err
}

// Connect 建立 MQTT 连接；配置了凭据则一并启用
func (p *MQTTPublisher) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	opts := paho.NewClientOptions().
		AddBroker(p.cfg.Broker).
		SetClientID(p.cfg.ClientID).
		SetUsername(p.cfg.Username).
		SetPassword(p.cfg.Password).
		SetAutoReconnect(true).                                  // paho 内建自动重连
		SetMaxReconnectInterval(time.Duration(p.cfg.RetryWait)*time.Second).
		SetOnConnectHandler(func(c paho.Client) {                // 重连成功回调
			log.Printf("[MQTT] 已连接 Broker %s", p.cfg.Broker)
		}).
		SetConnectionLostHandler(func(c paho.Client, err error) { // 掉线回调
			log.Printf("[MQTT] 连接断开: %v，将自动重连", err)
		})
	cli := paho.NewClient(opts)
	if token := cli.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("连接 MQTT Broker 失败: %w", token.Error())
	}
	// 重建连接前先断开旧客户端，避免反复重连时泄漏底层 TCP 连接
	if p.cli != nil {
		p.cli.Disconnect(250)
	}
	p.cli = cli
	return nil
}

// Publish 发布 JSON 载荷到配置主题；连接不可用时先重连一次再发布
func (p *MQTTPublisher) Publish(payload []byte) error {
	p.mu.Lock()
	cli := p.cli
	p.mu.Unlock()
	if cli == nil || !cli.IsConnectionOpen() {
		log.Println("[MQTT] 发布前发现连接不可用，重建连接...")
		if err := p.Connect(); err != nil {
			return err
		}
		p.mu.Lock()
		cli = p.cli
		p.mu.Unlock()
	}
	token := cli.Publish(p.cfg.Topic, p.cfg.QoS, false, payload)
	if token.WaitTimeout(5 * time.Second) && token.Error() != nil {
		return fmt.Errorf("MQTT 发布失败: %w", token.Error())
	}
	return nil
}

// Close 断开 MQTT 连接
func (p *MQTTPublisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cli != nil {
		p.cli.Disconnect(500)
	}
}

// ============================================================================
// 五、采集核心：轮询 -> 量纲换算 -> 组包
// ============================================================================

// convertRawValue 将 16 位寄存器原始值换算为工程量：value = raw*scale + offset
func convertRawValue(raw uint16, reg RegisterConfig) float64 {
	v := float64(raw)*reg.Scale + reg.Offset
	// 保留 3 位小数精度，避免浮点尾数噪声；
	// math.Round 对负数同样四舍五入（原 v*1000+0.5 强转写法在负数时会向零截断）
	return math.Round(v*1000) / 1000
}

// collectOnce 执行一轮完整采集：逐测点读取保持寄存器并换算
func collectOnce(mc *ModbusClient, cfg *GatewayConfig) (*Reading, error) {
	reading := NewReading(cfg.MQTT.ClientID)
	for _, reg := range cfg.Registers {
		raw, err := mc.ReadRegisters(reg)
		if err != nil {
			return nil, fmt.Errorf("读取寄存器 %s(地址%d) 失败: %w", reg.Name, reg.Address, err)
		}
		// 每个 16 位寄存器占 2 字节，大端序；先校验返回长度再取值，防止越界
		if len(raw) < int(reg.Quantity)*2 {
			return nil, fmt.Errorf("寄存器 %s(地址%d) 返回字节数不足: want %d, got %d",
				reg.Name, reg.Address, int(reg.Quantity)*2, len(raw))
		}
		value := uint16(raw[0])<<8 | uint16(raw[1])
		reading.Values[reg.Name] = convertRawValue(value, reg)
		reading.Units[reg.Name] = reg.Unit
	}
	return reading, nil
}

// collectorLoop 定时采集主循环：ticker 控制周期，失败时退避重连后继续
func collectorLoop(ctx context.Context, mc *ModbusClient, pub *MQTTPublisher, cfg *GatewayConfig) {
	interval := time.Duration(cfg.CollectInterval) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	failCount := 0 // 连续失败计数，用于日志降噪与退避
	for {
		select {
		case <-ctx.Done():
			log.Println("[采集] 收到退出信号，停止采集循环")
			return
		case <-ticker.C:
			// 先确保 Modbus 链路可用
			if err := mc.EnsureConnected(); err != nil {
				failCount++
				log.Printf("[采集] Modbus 重连失败(第%d次): %v", failCount, err)
				continue
			}
			reading, err := collectOnce(mc, cfg)
			if err != nil {
				failCount++
				log.Printf("[采集] 采集失败(连续%d次): %v", failCount, err)
				// 采集失败大概率是链路断了：主动关闭，下一轮触发重连
				mc.Close()
				continue
			}
			failCount = 0

			payload, err := reading.ToJSON()
			if err != nil {
				log.Printf("[采集] JSON 序列化失败: %v", err)
				continue
			}
			if err := pub.Publish(payload); err != nil {
				log.Printf("[采集] MQTT 发布失败: %v", err)
				continue
			}
			// 每秒一行采集日志，便于观察数据流通
			log.Printf("[采集] %s %v", time.Now().Format("15:04:05"), reading.Values)
		}
	}
}

// ============================================================================
// 六、程序入口：加载配置 -> 连接设备 -> 启动采集 -> 优雅退出
// ============================================================================

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("========== 工业物联网边缘网关启动 ==========")

	// 1. 加载配置文件（优先命令行参数指定，默认同目录 config.yaml）
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	log.Printf("[配置] 已加载 %s：Modbus=%s:%d，MQTT=%s，测点数=%d，周期=%dms",
		configPath, cfg.Modbus.Host, cfg.Modbus.Port, cfg.MQTT.Broker,
		len(cfg.Registers), cfg.CollectInterval)

	// 2. 建立 Modbus 连接（失败不退出，采集循环内会持续重连）
	modbusClient := NewModbusClient(cfg)
	if err := modbusClient.Connect(); err != nil {
		log.Printf("[警告] 初始 Modbus 连接失败，进入重连模式: %v", err)
	}
	defer modbusClient.Close()

	// 3. 建立 MQTT 连接（首连失败不退出：Publish 内部会在每次发布前重连）
	publisher, err := NewMQTTPublisher(&cfg.MQTT)
	if err != nil {
		log.Printf("[警告] 初始 MQTT 连接失败，发布时将自动重试: %v", err)
	}
	defer publisher.Close()

	// 4. 用 context 承载退出信号，保证协程能优雅结束
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 5. 启动定时采集协程
	go collectorLoop(ctx, modbusClient, publisher, cfg)

	// 6. 阻塞等待 Ctrl+C / SIGTERM，收到后取消 context 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("收到信号 %v，正在退出...", sig)
	cancel()
	time.Sleep(300 * time.Millisecond) // 给协程一点收尾时间
	log.Println("========== 网关已停止 ==========")
}
