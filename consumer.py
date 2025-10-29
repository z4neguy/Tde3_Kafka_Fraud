from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict
from db import Session
from models import Transacao
import logging
logging.getLogger("kafka").setLevel(logging.WARNING)



#  Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


#  Configuração do Consumer Kafka
consumer = KafkaConsumer(
    'transacoes',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # lê desde o início
    enable_auto_commit=True,
    group_id='fraude-detector'
)

logging.info("✅ Consumer conectado ao Kafka — ouvindo o tópico 'transacoes'...")


#-Histórico de transações por cliente
historico = defaultdict(list)


# Função de verificação de fraude
def verifica_fraude(transacao):
    cliente = transacao["client_id"]
    valor = transacao["amount"]
    cidade = transacao["city"]
    timestamp = datetime.fromisoformat(transacao["timestamp"])

    # 1️⃣ ALTO_VALOR
    if valor >= 10000:
        return "ALTO_VALOR"

    # Atualiza histórico do cliente
    historico[cliente].append((timestamp, cidade))

    # Remove transações antigas (mais de 10 min atrás)
    historico[cliente] = [
        (t, c) for (t, c) in historico[cliente]
        if timestamp - t <= timedelta(minutes=10)
    ]

    # 2️⃣ TEMPO_60s — 4 transações em < 60s
    ultimas_60s = [t for (t, _) in historico[cliente] if timestamp - t <= timedelta(seconds=60)]
    if len(ultimas_60s) >= 4:
        return "TEMPO_60s"

    # 3️⃣ GEO_10m — cidades diferentes em < 10 minutos
    cidades_ultimas = {c for (t, c) in historico[cliente] if timestamp - t <= timedelta(minutes=10)}
    if len(cidades_ultimas) >= 2:
        return "GEO_10m"

    return None

# ===============================
# Loop principal
# ===============================
session = Session()


try:
    for msg in consumer:
        transacao = msg.value
        fraude = verifica_fraude(transacao)

        # Salva no banco
        nova_transacao = Transacao(
            transaction_id=transacao["transaction_id"],
            client_id=transacao["client_id"],
            amount=transacao["amount"],
            city=transacao["city"],
            timestamp=datetime.fromisoformat(transacao["timestamp"]),
            tipo_fraude=fraude
        )
        session.add(nova_transacao)
        session.commit()

        # Logging
        if fraude:
            logging.warning(f"🚨 FRAUDE DETECTADA: {fraude} | "
                            f"Cliente: {transacao['client_id']} | "
                            f"Valor: {transacao['amount']} | "
                            f"Cidade: {transacao['city']}")
        else:
            logging.info(f"✅ Transação normal: Cliente {transacao['client_id']} - "
                         f"{transacao['amount']} em {transacao['city']}")

except KeyboardInterrupt:
    logging.info("🛑 Encerrando consumer...")
    session.close()
