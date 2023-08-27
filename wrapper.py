import paho.mqtt.client as mqtt
import time
import threading
import json
import queue

# MQTT定数
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
KEEP_ARIVE = 60
TOPIC_SUB_EL = "root/sub/el"
TOPIC_PUB_EL = "root/pub/el"
TOPIC_SUB_TEST = "root/sub/test"
TOPIC_PUB_TEST = "root/pub/test"

# スレーブトーカのアドレス定数
START_TOKER_ADDRESS = 0x040 # 64
# START_TOKERR_ADDRESS = 0x000 # 00
STOP_TOKER_ADDRESS = 0x2BF # 703

# キューf
serial_to_mqtt_queue = queue.Queue()
mqtt_to_serial_queue = queue.Queue()
config_queue = queue.Queue()

## mqtt subの設定 --------
# ブローカーに接続できたときの処理
def on_connect(client, userdata, flag, rc):
  print("  {}: Connected with result code {}".format(threading.current_thread().getName(),str(rc)))  # 接続できた旨表示
  client.subscribe("drone/001")  # subするトピックを設定 

# ブローカーに接続できたときの処理
def on_connect_sub_el(client, userdata, flag, rc):
  print("  {}: Connected with result code {}".format(threading.current_thread().getName(),str(rc)))  # 接続できた旨表示
  client.subscribe(TOPIC_SUB_EL)  # subするトピックを設定 

# ブローカーに接続できたときの処理
def on_connect_sub_test(client, userdata, flag, rc):
  print("  {}: Connected with result code {}".format(threading.current_thread().getName(),str(rc)))  # 接続できた旨表示
  client.subscribe(TOPIC_SUB_TEST)  # subするトピックを設定 

## ブローカーが切断したときの処理
def on_disconnect_sub(client, userdata, rc):
  if  rc != 0:
    print("  {}: Unexpected disconnection.".format(threading.current_thread().getName()))

## メッセージが届いたときの処理
def on_message(client, userdata, msg):
  # msg.topicにトピック名が，msg.payloadに届いたデータ本体が入っている
  print("  {}: Received message '{} ' on topic ' {} ` with QoS '{}".format(threading.current_thread().getName(), str(msg.payload) ,msg.topic, str(msg.qos)))

## EL制御モックからメッセージが届いたときの処理
def on_message_sub_from_EL(client, userdata, msg):
  global serial_to_mqtt_queue
  print("  {}: EL制御基盤モックから受信しました")
  print("  {}: Received message '{} ' on topic ' {} ` with QoS '{}".format(threading.current_thread().getName(), str(msg.payload) ,msg.topic, str(msg.qos)))
  # serial_to_mqtt_queueにデータを追加する
  address = str(msg.payload)
  serial_to_mqtt_queue.put(address)
  client.disconnect()

## テストシナリオからメッセージが届いたときの処理
def on_message_sub_from_Test(client, userdata, msg):
  global mqtt_to_serial_queue
  print("  {}: テストシナリオから受信しました")
  print("  {}: Received message '{} ' on topic ' {} ` with QoS '{}".format(threading.current_thread().getName(), str(msg.payload) ,msg.topic, str(msg.qos)))
  # serial_to_mqtt_queueにデータを追加する
  data = str(msg.payload)
  mqtt_to_serial_queue.put(data)
  client.disconnect()

# ------------------------

## mqtt pubの設定^^^^^^^^^^
## ブローカーに接続できたときの処理
def on_connect_pub(client, userdata, flag, rc):
  print("  {}: Connected with result code {}".format(threading.current_thread().getName(), str(rc)))

## ブローカーが切断したときの処理
def on_disconnect_pub(client, userdata, rc):
  if rc != 0:
     print("  {}: Unexpected disconnection.".format(threading.current_thread().getName()))

## publishが完了したときの処理
def on_publish(client, userdata, mid):
  print("  {}: publish: {}".format(threading.current_thread().getName(), mid))
## ^^^^^^^^^^^^^^^^^^^^^^^

# Thread: Serial_driverの処理
def serial_driver(start_addr, stop_addr):
    global mqtt_to_serial_queue

    print("  {}: Serialドライバーの開始".format(threading.current_thread().getName()))
    # EL制御基盤モックからの受信

    while True:
        # MQTTの接続設定
        client_sub = mqtt.Client()                 # クラスのインスタンス(実体)の作成
        client_sub.on_connect = on_connect_sub_el         # 接続時のコールバック関数を登録
        client_sub.on_disconnect = on_disconnect_sub   # 切断時のコールバックを登録
        client_sub.on_message = on_message_sub_from_EL         # メッセージ到着時のコールバック
        
        client_sub.connect("localhost", 1883, 60)  # 接続先は自分自身
        client_sub.loop_forever()
        # EL制御基盤モックからの受信を完了
        print("  {}: EL制御基盤モックからの受信を完了".format(threading.current_thread().getName()))

        # mqtt_to_serial_queueからデータを取り出す
        print("  {}: serial_to_mqtt_queueの長さ: {}".format(threading.current_thread().getName(), serial_to_mqtt_queue.qsize()))

        data = mqtt_to_serial_queue.get()
        print("  {}: mqtt_to_serial_queueから取り出したデータ: {}".format(threading.current_thread().getName(), data))

        # データをEL制御基盤モックに送信する
        client_pub = mqtt.Client()                 # クラスのインスタンス(実体)の作成
        client_pub.on_connect = on_connect_pub         # 接続時のコールバック関数を登録
        client_pub.on_disconnect = on_disconnect_pub   # 切断時のコールバックを登録
        client_pub.on_publish = on_publish         # メッセージ送信時のコールバック
        
        client_pub.connect("localhost", 1883, 60)  # 接続先は自分自身
        # 通信処理スタート
        client_pub.loop_start()
        client_pub.publish(TOPIC_PUB_EL,"00")    # トピック名とメッセージを決めて送信
        client_pub.disconnect()

# Thread: Mqtt_driverの処理
def mqtt_driver():
    global serial_to_mqtt_queue

    print("  {}: MQTTドライバーの開始".format(threading.current_thread().getName()))


    while True:
        # serial_to_mqtt_queueからアドレスを読み込む
        address = serial_to_mqtt_queue.get()
        print("  {}: serial_to_mqtt_queueから取り出したデータ: {}".format(threading.current_thread().getName(), address))
    
        # データをEL制御基盤モックに送信する
        client_pub = mqtt.Client()                 # クラスのインスタンス(実体)の作成
        client_pub.on_connect = on_connect_pub         # 接続時のコールバック関数を登録
        client_pub.on_disconnect = on_disconnect_pub   # 切断時のコールバックを登録
        client_pub.on_publish = on_publish         # メッセージ送信時のコールバック
        
        client_pub.connect("localhost", 1883, 60)  # 接続先は自分自身
        # 通信処理スタート
        client_pub.loop_start()
        client_pub.publish(TOPIC_SUB_TEST,"00")    # トピック名とメッセージを決めて送信
        client_pub.disconnect()
        # テストシナリオへの送信を終了

        # テストシナリオからデータを受信
        # MQTTの接続設定
        client_sub = mqtt.Client()                 # クラスのインスタンス(実体)の作成
        client_sub.on_connect = on_connect_sub_test         # 接続時のコールバック関数を登録
        client_sub.on_disconnect = on_disconnect_sub   # 切断時のコールバックを登録
        client_sub.on_message = on_message_sub_from_Test         # メッセージ到着時のコールバック
        
        client_sub.connect("localhost", 1883, 60)  # 接続先は自分自身
        client_sub.loop_forever()
        # テストシナリオからの受信を完了
        print("  {}: テストシナリオからの受信を完了".format(threading.current_thread().getName()))     


# Mainの処理
def main():
    print("------------------")
    print("{}: ラッパーの起動と開始".format(threading.current_thread().getName()))
    # threadインスタンスの生成
    thread_serial = threading.Thread(name="SerialThread", target=serial_driver, args=(START_TOKER_ADDRESS, STOP_TOKER_ADDRESS))
    thread_mqtt = threading.Thread(name="MqttThread", target=mqtt_driver)

    thread_serial.start()
    thread_mqtt.start()


if __name__ == "__main__":
    main()

