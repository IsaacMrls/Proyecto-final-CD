#!/usr/bin/env python3
"""
Worker (Consumidor) para simulación Montecarlo.
Recibe la expresión matemática y procesa escenarios.
"""

import pika
import json
import time
import math
import socket


RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "Isaac_mrls"
RABBITMQ_PASSWORD = "876435"
RABBITMQ_VHOST = "/"

QUEUE_MODEL = "cola_modelo"
QUEUE_SCENARIOS = "cola_escenarios"
QUEUE_RESULTS = "cola_resultados" # Nueva cola para enviar al dashboard

class MonteCarloWorker:
    def __init__(self):
        self.worker_id = f"worker_{socket.gethostname()}_{time.time()}"
        self.expression = None
        self.model_id = None
        
        # Conexión RabbitMQ
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, 
            virtual_host=RABBITMQ_VHOST, credentials=credentials
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        
        # Declarar colas 
        self.channel.queue_declare(queue=QUEUE_MODEL, durable=True)
        self.channel.queue_declare(queue=QUEUE_SCENARIOS, durable=True)
        self.channel.queue_declare(queue=QUEUE_RESULTS, durable=True)
        
        # Configurar QoS para no saturar al worker (1 mensaje a la vez)
        self.channel.basic_qos(prefetch_count=1)

    def obtener_modelo(self):
        """Intenta obtener el modelo de la cola. Bloquea hasta conseguirlo."""
        print(f"[{self.worker_id}] Buscando definición del modelo...")
        
        while self.expression is None:
            # Usamos basic_get para polling controlado del modelo
            method, properties, body = self.channel.basic_get(queue=QUEUE_MODEL, auto_ack=False)
            
            if method:
                data = json.loads(body)
                self.expression = data['expression']
                self.model_id = data['model_id']
                print(f"[{self.worker_id}] Modelo cargado: {self.model_id}")
                print(f"[{self.worker_id}] Expresión a evaluar: {self.expression}")
                

                # Si hay múltiples workers, todos necesitan leer el modelo. 
                # Al devolverlo a la cola, otros workers pueden leerlo también.
                
                self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                time.sleep(1) # Esperar un poco antes de reintentar

    def evaluar_escenario(self, variables):
        """Evalúa la expresión matemática de forma segura."""
        # Contexto matemático seguro (permitimos funciones de math)
        safe_dict = {k: getattr(math, k) for k in dir(math) if not k.startswith("_")}
        
        # Variables del escenario (a, b, x, y...)
        safe_dict.update(variables)
        
        try:
            # Evaluamos la expresión string
            return eval(self.expression, {"__builtins__": {}}, safe_dict)
        except Exception as e:
            print(f"Error evaluando: {e}")
            return None

    def callback_escenario(self, ch, method, properties, body):
        if not self.expression:
            print("Error: Llegó un escenario pero no tengo modelo.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        scenario = json.loads(body)
        vars_values = scenario['variables']
        
        # 1. Ejecutar el modelo
        resultado_valor = self.evaluar_escenario(vars_values)
        
        if resultado_valor is not None:
            # 2. Construir mensaje de resultado
            resultado_msg = {
                "type": "result",
                "scenario_id": scenario['scenario_id'],
                "worker_id": self.worker_id,
                "result": resultado_valor,
                "timestamp": time.time()
            }
            
            # 3. Publicar resultado
            ch.basic_publish(
                exchange='',
                routing_key=QUEUE_RESULTS,
                body=json.dumps(resultado_msg),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            # 4. Confirmar procesamiento 
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if scenario['scenario_id'] % 50 == 0:
                print(f"[{self.worker_id}] Procesado ID {scenario['scenario_id']} -> {resultado_valor:.4f}")

    def run(self):
        self.obtener_modelo()
        print(f"[{self.worker_id}] Esperando escenarios en '{QUEUE_SCENARIOS}'...")
        
        self.channel.basic_consume(
            queue=QUEUE_SCENARIOS, 
            on_message_callback=self.callback_escenario
        )
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.connection.close()
            print("Worker detenido.")

if __name__ == "__main__":
    worker = MonteCarloWorker()
    worker.run()
