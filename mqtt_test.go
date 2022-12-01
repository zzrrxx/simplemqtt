package simplemqtt

import (
	"testing"
	"time"
)

const server = "192.168.66.102:51883"

func TestMQTT_Connect(t *testing.T) {
	cfg := &Config{
		ClientId:     "abc",
	}

	mqtt := NewMQTT(cfg)

	mqtt.Connect(server)

	msg := Message{
		DUP:              false,
		QoS:              0,
		RETAIN:           false,
		TopicName:        "TestTopic",
		Data:             []byte("hello"),
	}

	mqtt.Publish(&msg)

	mqtt.Disconnect()
}


func TestMQTT_PublishQoS1Message(t *testing.T) {
	cfg := &Config{
		ClientId:     "abc",
	}

	mqtt := NewMQTT(cfg)
	mqtt.Connect(server)

	msg := Message{
		QoS:              2,
		TopicName:        "TestTopic",
		Data:             []byte("hello"),
	}

	mqtt.Publish(&msg)
	mqtt.Disconnect()
}

func TestMQTT_Subscribe(t *testing.T) {
	cfg := &Config{
		ClientId:     "abc",
	}

	mqtt := NewMQTT(cfg)
	mqtt.Connect(server)

	mqtt.Subscribe([]string{"TestTopic"}, []byte{ 2 })
	mqtt.Unsubscribe([]string{"TestTopic"})

	mqtt.Ping()


	mqtt.Disconnect()
}



func TestMQTT_Test(t *testing.T) {



	go func() {

		time.Sleep(time.Second)
		cfg := &Config{
			ClientId:     "abc",
		}
		mqtt := NewMQTT(cfg)
		mqtt.Connect(server)

		msg := Message{
			DUP:              false,
			QoS:              2,
			RETAIN:           true,
			TopicName:        "TestTopic",
			Data:             []byte("IIII IIII IIII"),
		}

		mqtt.Publish(&msg)

		println("Published");
	}()

	go func() {
		cfg := &Config{
			ClientId:     "abc",
		}
		mqtt := NewMQTT(cfg)
		mqtt.Connect(server)

		mqtt.Subscribe([]string{"TestTopic"}, []byte{2})

		msg, _ := mqtt.ReceiveMessage(10000)
		print(string(msg.Data))

	}()



	time.Sleep(10000 * time.Second)

}
