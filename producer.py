from kafka import KafkaProducer
import json, random, uuid, time
from datetime import datetime

def gerar_transacao():
    cidades = ["São Paulo", "Rio de Janeiro", "Manaus", "Belém", "Fortaleza"]
    transacao = {
        "transaction_id": str(uuid.uuid4()),
        "client_id": random.randint(1, 10),
        "amount": round(random.uniform(100, 20000), 2),
        "city": random.choice(cidades),
        "timestamp": datetime.now().isoformat()
    }
    return transacao

def main():
    # Conecta ao servidor Kafka local
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],  # endereço do Kafka
        value_serializer=lambda v: json.dumps(v).encode("utf-8")  # converte o dicionário pra JSON
    )

    topic = "transacoes"

    print("✅ Producer iniciado. Enviando transações a cada 3 segundos...")

    try:
        while True:
            transacao = gerar_transacao()
            producer.send(topic, value=transacao)
            print(f"📤 Enviada: {transacao}")
            time.sleep(3)
    except KeyboardInterrupt:
        print("\n🚪 Encerrando producer...")
        producer.close()

if __name__ == "__main__":
    main()