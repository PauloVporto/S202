import threading
import random
import time
from pymongo import MongoClient, errors

# Conectando ao MongoDB
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client['S202']
    sensores_collection = db['sensores']
    print("Conexão com MongoDB estabelecida com sucesso!")
except errors.ConnectionFailure as e:
    print(f"Erro ao conectar ao MongoDB: {e}")


# Função para simular um sensor
def simular_sensor(nome_sensor):
    sensor_alarmado = False

    while not sensor_alarmado:
        valor_sensor = random.uniform(30, 40)

        if valor_sensor > 38:
            sensor_alarmado = True
            resultado = sensores_collection.update_one(
                {"nomeSensor": nome_sensor},
                {"$set": {"valorSensor": valor_sensor, "sensorAlarmado": True}}
            )
            if resultado.matched_count > 0:
                print(f"Atenção! Temperatura muito alta! Verificar Sensor {nome_sensor}!")
            else:
                print(f"Erro ao atualizar o sensor {nome_sensor}.")
        else:
            resultado = sensores_collection.update_one(
                {"nomeSensor": nome_sensor},
                {"$set": {"valorSensor": valor_sensor}}
            )
            if resultado.matched_count > 0:
                print(f"{nome_sensor} -> Temperatura: {valor_sensor:.2f} C°")
            else:
                print(f"Erro ao atualizar o sensor {nome_sensor}.")

        time.sleep(2)


# Inserindo documentos para cada sensor no MongoDB
try:
    sensores_collection.insert_many([
        {"nomeSensor": "Temp1", "valorSensor": None, "unidadeMedida": "C°", "sensorAlarmado": False},
        {"nomeSensor": "Temp2", "valorSensor": None, "unidadeMedida": "C°", "sensorAlarmado": False},
        {"nomeSensor": "Temp3", "valorSensor": None, "unidadeMedida": "C°", "sensorAlarmado": False}
    ])
    print("Documentos dos sensores criados com sucesso!")
except errors.WriteError as e:
    print(f"Erro ao inserir documentos dos sensores: {e}")

# Criando e iniciando as threads para cada sensor
threads = []
for nome_sensor in ["Temp1", "Temp2", "Temp3"]:
    thread = threading.Thread(target=simular_sensor, args=(nome_sensor,))
    threads.append(thread)
    thread.start()

# Aguardando todas as threads finalizarem
for thread in threads:
    thread.join()
