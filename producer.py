from kafka import KafkaProducer
import json, random, uuid, time
from datetime import datetime

# Limite de transações suspeitas consecutivas
MAX_FRAUD_CONSECUTIVAS = 5
fraud_consecutivas = 0

def gerar_transacao():
    global fraud_consecutivas
    
    cidades = ["São Paulo", "Rio de Janeiro", "Manaus", "Belém", "Fortaleza"]

    # Define valor da transação
    if fraud_consecutivas >= MAX_FRAUD_CONSECUTIVAS:
        # Se já teve muitas suspeitas seguidas, força um valor "normal"
        amount = round(random.uniform(100, 5000), 2)
        fraud_consecutivas = 0
    else:
        # Valor pode ser grande (possível fraude)
        amount = round(random.uniform(100, 20000), 2)
        if amount > 15000:
            fraud_consecutivas += 1
        else:
            fraud_consecutivas = 0

    transacao = {
        "transaction_id": str(uuid.uuid4()),
        "client_id": random.randint(1, 10),
        "amount": amount,
        "city": random.choice(cidades),
        "timestamp": datetime.now().isoformat()
    }
    return transacao

def main():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
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
