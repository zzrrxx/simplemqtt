package simplemqtt

import (
	"errors"
	"net"
	"strconv"
)

const (
	pktConnect     = 1
	pktConnAck     = 2
	pktPublish     = 3
	pktPubAck      = 4
	pktPubRec      = 5
	pktPubRel      = 6
	pktPubComp     = 7
	pktSubscribe   = 8
	pktSubAck      = 9
	pktUnsubscribe = 10
	pktUnSubAck    = 11
	pktPingReq     = 12
	pktPingResp    = 13
	pktDisconnect  = 14
)

var mqttPktNames = []string{
	"CONNECT",
	"CONNACK",
	"PUBLISH",
	"PUBACK",
	"PUBREC",
	"PUBREL",
	"PUBCOMP",
	"SUBSCRIBE",
	"SUBACK",
	"UNSUBSCRIBE",
	"UNSUBACK",
	"PINGREQ",
	"PINGRESP",
	"DISCONNECT",
}

type Config struct {
	ClientId     string
	User         string
	Password     string
	CleanSession bool
	KeepAlive    int // seconds

	WillTopic    string
	WillQoS      int
	WillRetain   bool
	WillMessage  string
}

type Message struct {
	DUP              bool
	QoS              byte // [0, 1, 2]
	RETAIN           bool
	TopicName        string
	Data             []byte

	PacketIdentifier int16 // this is for receiving. No need to set it when publishing messages
}

type MQTT struct {
	Config     *Config

	_conn      net.Conn
	_raddr     *net.TCPAddr
	_laddr     *net.TCPAddr

	_nextPktId int16
}

func NewMQTT(cfg *Config) *MQTT {
	return &MQTT{
		Config: cfg,
		_conn:  nil,
		_raddr: nil,
		_laddr: nil,
	}
}

func (this *MQTT) Connect(server string) error {
	var err error
	if err = this.validateConfig(); err != nil {
		return err
	}
	if this._raddr, err = net.ResolveTCPAddr("tcp", server); err != nil {
		return err
	}
	if this._conn, err = net.DialTCP("tcp", nil, this._raddr); err != nil {
		return err
	}

	data, err := this.buildPacketConnect()
	if err != nil {
		return err
	}
	if err = this.mustWrite(data); err != nil {
		return err
	}

	b, _, _, err := this.readControlPacket()
	if err != nil {
		return err
	}
	if err = this.ensurePackageType(pktConnAck, b); err != nil {
		return err
	}
	return nil
}
func (this *MQTT) Disconnect() error {
	if this._conn == nil {
		return errors.New("Not connected")
	}

	defer func() {
		this._conn = nil
	}()

	data := this.buildControlPacket(pktDisconnect, 0, nil)
	if err := this.mustWrite(data); err != nil {
		return err
	}

	if err := this._conn.Close(); err != nil {
		return err
	}

	return nil
}
func (this *MQTT) Publish(msg *Message) error {
	if len(msg.TopicName) == 0 {
		return errors.New("TopicName cannot be empty")
	}
	if err := this.validateQoS(msg.QoS); err != nil {
		return err
	}

	var flags byte = 0
	if msg.RETAIN {
		flags |= 1
	}
	if msg.QoS > 0 {
		flags |= (msg.QoS << 1)
	}
	if msg.DUP {
		flags |= 1 << 3
	}

	msg.PacketIdentifier = int16(this.nextPacketId())

	data := make([]byte, 0)
	data = encodeInt16String(data, msg.TopicName)
	data = append(data, encodeInt16(int(msg.PacketIdentifier))...)
	data = append(data, msg.Data...)
	data = this.buildControlPacket(pktPublish, flags, data)

	if err := this.mustWrite(data); err != nil {
		return err
	}

	if msg.QoS == 0 {
		return nil // no response for QoS 0
	} else if msg.QoS == 1 {
		b, _, _, err := this.readControlPacket()
		if err != nil {
			return err
		}
		if err = this.ensurePackageType(pktPubAck, b); err != nil {
			return err
		}
		return nil
	} else if msg.QoS == 2 {
		b, remainingLength, pktId, err := this.readControlPacket()
		if err != nil {
			return err
		}
		if err = this.ensurePackageType(pktPubRec, b); err != nil {
			return err
		}
		if remainingLength != 2 {
			return errors.New(this.pktName(pktPubRel) + " size is not correct")
		}

		pubRelData := this.buildControlPacket(pktPubRel, 0, pktId)
		if err = this.mustWrite(pubRelData); err != nil {
			return err
		}

		b, remainingLength, pktId, err = this.readControlPacket()
		if err != nil {
			return err
		}
		if err = this.ensurePackageType(pktPubComp, b); err != nil {
			return err
		}
	}

	return nil
}
func (this *MQTT) Subscribe(topicFilters []string, requestedQoSs []byte) (ackedQoS []byte, err error) {
	data, err := this.buildPacketSubscribe(topicFilters, requestedQoSs)
	if err != nil {
		return nil, err
	}
	data = this.buildControlPacket(pktSubscribe, 0, data)

	if err = this.mustWrite(data); err != nil {
		return nil, err
	}

	b, _, payload, err := this.readControlPacket()
	if err != nil {
		return nil, err
	}
	if err = this.ensurePackageType(pktSubAck, b); err != nil {
		return nil, err
	}

	payload = payload[2:] // skip the packet identifier
	for _, v := range data {
		if v == 0 || v == 1 || v == 2 {
			ackedQoS = append(ackedQoS, v)
		} else {
			return nil, errors.New("Subscribe failure")
		}
	}
	return
}
func (this *MQTT) Unsubscribe(topicFilters []string) error {
	data, err := this.buildPacketUnsubscribe(topicFilters)
	if err != nil {
		return err
	}
	data = this.buildControlPacket(pktUnsubscribe, 0, data)

	if err = this.mustWrite(data); err != nil {
		return err
	}

	b, _, _, err := this.readControlPacket()
	if err != nil {
		return err
	}
	if err = this.ensurePackageType(pktUnSubAck, b); err != nil {
		return err
	}

	return nil
}
func (this *MQTT) Ping() error {
	data := this.buildControlPacket(pktPingReq, 0, nil)
	if err := this.mustWrite(data); err != nil {
		return err
	}

	b, _, _, err := this.readControlPacket()
	if err != nil {
		return err
	}
	if err = this.ensurePackageType(pktPingResp, b); err != nil {
		return err
	}
	return nil
}

func (this *MQTT) ReceiveMessage(timeoutMS int) (*Message, error) {
	b, _, payload, err := this.readControlPacket()
	if err != nil {
		return nil, err
	}
	if err = this.ensurePackageType(pktPublish, b); err != nil {
		return nil, err
	}

	msg := &Message{
		//TopicName:        ,
		//Data:             nil,
		//PacketIdentifier: 0,
	}

	if b&0x01 != 0 {
		msg.RETAIN = true
	}
	if b&0x06 != 0 {
		msg.QoS = (b & 0x06) >> 1
	}
	if b&0x08 != 0 {
		msg.DUP = true
	}

	if err = this.validateQoS(msg.QoS); err != nil {
		return nil, err
	}

	if msg.TopicName, err = decodeInt16String(payload); err != nil {
		return nil, err
	}
	off := 2 + len(msg.TopicName)
	if pktId, err := decodeInt16(payload[off:]); err != nil {
		return nil, err
	} else {
		msg.PacketIdentifier = int16(pktId)
	}

	l := len(payload) - off - 2
	msg.Data = make([]byte, l)
	copy(msg.Data, payload[off + 2:])

	if msg.QoS == 0 {
		return msg, nil
	} else if msg.QoS == 1 {
		pkt := this.buildControlPacket(pktPubAck, 0, encodeInt16(int(msg.PacketIdentifier)))
		if err = this.mustWrite(pkt); err != nil {
			return msg, err
		}
	} else if msg.QoS == 2 {
		pkt := this.buildControlPacket(pktPubRec, 0, encodeInt16(int(msg.PacketIdentifier)))
		if err = this.mustWrite(pkt); err != nil {
			return msg, err
		}
		b, _, payload, err = this.readControlPacket()
		if err != nil {
			return msg, err
		}
		if err = this.ensurePackageType(pktPubRel, b); err != nil {
			return msg, err
		}
		pkt = this.buildControlPacket(pktPubComp, 0, encodeInt16(int(msg.PacketIdentifier)))
		if err = this.mustWrite(pkt); err != nil {
			return msg, err
		}
	}
	return msg, nil
}

func (this *MQTT) buildPacketConnect() ([]byte, error) {
	connFlagIndex := 7

	pkt := make([]byte, 0)
	pkt = append(pkt, []byte{0x00, 0x04, 'M', 'Q', 'T', 'T'}...) // Protocol Name
	pkt = append(pkt, 4)                                         // Protocol Level

	pkt = append(pkt, byte(0)) // we will fill in the flag at the end
	pkt = append(pkt, encodeInt16(this.Config.KeepAlive)...)

	pkt = encodeInt16String(pkt, this.Config.ClientId)
	if len(this.Config.WillTopic) > 0 {
		pkt[connFlagIndex] |= (1 << 2) // will flag
		pkt[connFlagIndex] |= (byte(this.Config.WillQoS) << 3)
		if this.Config.WillRetain {
			pkt[connFlagIndex] |= (1 << 5)
		}
		pkt = encodeInt16String(pkt, this.Config.WillTopic)
		pkt = encodeInt16String(pkt, this.Config.WillMessage)
	}
	if len(this.Config.User) > 0 {
		pkt[connFlagIndex] |= (1 << 7)
		pkt = encodeInt16String(pkt, this.Config.User)
	}
	if len(this.Config.Password) > 0 {
		pkt[connFlagIndex] |= (1 << 6)
		pkt = encodeInt16String(pkt, this.Config.Password)
	}
	if this.Config.CleanSession {
		pkt[connFlagIndex] |= (1 << 1)
	}

	return this.buildControlPacket(pktConnect, 0, pkt), nil
}
func (this *MQTT) buildPacketSubscribe(topicFilters []string, requestedQoSs []byte) ([]byte, error) {
	if len(topicFilters) != len(requestedQoSs) {
		return nil, errors.New("TopicFilters' length does not match with RequestedQoSs' length")
	}
	if len(topicFilters) == 0 {
		return nil, errors.New("TopicFilters/RequestedQoSs cannot be nil")
	}

	pkt := make([]byte, 0)
	pkt = append(pkt, encodeInt16(this.nextPacketId())...)

	for i, _ := range topicFilters {
		if err := this.validateTopicFilter(topicFilters[i]); err != nil {
			return nil, err
		}
		if err := this.validateQoS(requestedQoSs[i]); err != nil {
			return nil, err
		}

		pkt = encodeInt16String(pkt, topicFilters[i])
		pkt = append(pkt, requestedQoSs[i])
	}

	return pkt, nil
}
func (this *MQTT) buildPacketUnsubscribe(topicFilters []string) ([]byte, error) {
	if len(topicFilters) == 0 {
		return nil, errors.New("TopicFilters cannot be nil")
	}

	pkt := make([]byte, 0)
	pkt = append(pkt, encodeInt16(this.nextPacketId())...)

	for _, t := range topicFilters {
		if err := this.validateTopicFilter(t); err != nil {
			return nil, err
		}
		pkt = encodeInt16String(pkt, t)
	}
	return pkt, nil
}
func (this *MQTT) buildControlPacket(typ byte, flags byte, data []byte) []byte {
	pkt := make([]byte, 0)
	if typ == pktPubRel || typ == pktSubscribe || typ == pktUnsubscribe {
		flags = 2
	} else if typ == pktPublish {
		// keep the input flags
	} else {
		flags = 0
	}
	pkt = append(pkt, typ << 4 | flags)
	pkt = append(pkt, encodeRemainingLength(len(data))...)
	pkt = append(pkt, data...)
	return pkt
}

func (this *MQTT) readControlPacket() (firstByte byte, remainingLength int, payload []byte, err error) {
	ret := make([]byte, 2)

	// all mqtt packets have at least 2 bytes
	if err = this.mustRead(ret, 2); err != nil {
		return
	}
	firstByte = ret[0]

	for ret[len(ret)-1]&0x80 != 0 {
		b := []byte{0}
		if err = this.mustRead(b, 1); err != nil {
			return
		}
		ret = append(ret, b[0])
	}

	if remainingLength, _, err = decodeRemainingLength(ret[1:]); err != nil {
		return
	}

	payload = make([]byte, remainingLength)
	if err = this.mustRead(payload, remainingLength); err != nil {
		return
	}
	return
}

func (this *MQTT) mustRead(buf []byte, length int) error {
	lenRead := 0
	for lenRead < length {
		cnt, err := this._conn.Read(buf[lenRead:])
		if err != nil {
			return err
		}
		lenRead += cnt
	}
	return nil
}
func (this *MQTT) mustWrite(buf []byte) error {
	lenWrite := 0
	for lenWrite < len(buf) {
		cnt, err := this._conn.Write(buf[lenWrite:])
		if err != nil {
			return err
		}
		lenWrite += cnt
	}
	return nil
}
func (this *MQTT) validateConfig() error {
	if len(this.Config.ClientId) == 0 {
		return errors.New("ClientId is required")
	}
	if this.Config.WillQoS < 0 || this.Config.WillQoS > 2 {
		return errors.New("Invalid WillQoS value")
	}
	if this.Config.KeepAlive < 0 {
		return errors.New("Invalid KeepAlive value")
	}
	return nil
}
func (this *MQTT) ensurePackageType(expectedType byte, pktFirstByte byte) error {
	pktType := (pktFirstByte & 0xF0) >> 4
	if pktType != expectedType {
		// TODO: drop the connection
		return errors.New("Invalid packet: expected packet " + this.pktName(expectedType) + ", but received packet " + this.pktName(pktType))
	}
	return nil
}
func (this *MQTT) validateQoS(val byte) error {
	if val == 0 || val == 1 || val == 2 {
		return nil
	}
	return errors.New("Invalid QoS value: " + strconv.FormatInt(int64(val), 10))
}
func (this *MQTT) validateTopicFilter(val string) error {
	return nil // TODO
}
func (this *MQTT) pktName(typ byte) string {
	if typ >= pktConnect && typ <= pktDisconnect {
		return mqttPktNames[typ]
	}
	return "UNKNOWN(" + strconv.FormatInt(int64(typ), 10) + ")"
}
func (this *MQTT) nextPacketId() int {
	ret := this._nextPktId
	this._nextPktId++
	return int(ret)
}

func encodeInt16(val int) []byte {
	b1 := (val & 0xFF00) >> 8
	b2 := (val & 0x00FF)
	return []byte{byte(b1), byte(b2)}
}
func encodeInt16String(dst []byte, val string) []byte {
	dst = append(dst, encodeInt16(len(val))...)
	return append(dst, []byte(val)...)
}
func decodeInt16(val []byte) (int, error) {
	if len(val) < 2 {
		return 0, errors.New("Invalid packet identifier bytes length")
	}
	return int(val[0] << 8) | int(val[1]), nil
}
func decodeInt16String(val []byte) (string, error) {
	l, err := decodeInt16(val)
	if err != nil {
		return "", err
	}
	if len(val)-2 < l {
		return "", errors.New("Invalid string encoding")
	}
	return string(val[2 : 2+l]), nil
}
func encodeRemainingLength(val int) []byte {
	encoded := make([]byte, 0)
	for val > 0 {
		b := val % 128
		val = val / 128
		if val > 0 { // if there are more data to encode, set the top bit of this byte
			b |= 128
		}
		encoded = append(encoded, byte(b))
	}
	if len(encoded) == 0 {
		encoded = append(encoded, 0)
	}
	return encoded
}
func decodeRemainingLength(val []byte) (value int, len int, err error) {
	multiplier := 1
	value = 0
	len = 0

	for {
		b := val[len]
		len++
		value += int(b & 127) * multiplier
		multiplier *= 128

		if multiplier > 128 * 128 * 128 {
			return 0, 0, errors.New("Malformed Remaining Length")
		}

		if b & 128 == 0 {
			break
		}
	}
	return
}
