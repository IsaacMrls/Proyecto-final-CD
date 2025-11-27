#!/usr/bin/env python3

import pika
import json
import random

RABBITMQ_HOST = "192.168.1.75"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "Isaac_mrls"
RABBITMQ_PASSWORD = "876435"
RABBITMQ_VHOST = "/"

COLA_TAREAS = "tareas_montecarlo"
COLA_RESULTADOS = "resultados_montecarlo"

class WorkerMontecarlo:
    
    def __init__(self):  # ✓ Corregido: doble guion bajo
        self.id_worker = f"worker_{random.randint(1000, 9999)}"

        # Agregar credenciales como en producer.py
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials
        )
        
        self.conexion = pika.BlockingConnection(params)
        self.canal = self.conexion.channel()
        
        self.canal.queue_declare(queue=COLA_TAREAS, durable=True)
        self.canal.queue_declare(queue=COLA_RESULTADOS, durable=True)
        
        print(f"Worker {self.id_worker} listo!")
    
    def calcular_pi(self, num_puntos):
        puntos_dentro = 0
        
        for i in range(num_puntos):
            x = random.random()  
            y = random.random()
            
            if x*x + y*y <= 1:
                puntos_dentro += 1
        
        pi_aproximado = 4 * puntos_dentro / num_puntos
        return pi_aproximado
    
    def procesar_mensaje(self, canal, metodo, propiedades, cuerpo):
        print(f"Recibí mensaje: {cuerpo}")
        
        try:
            datos = json.loads(cuerpo)
            num_puntos = datos.get("puntos", 1000)
            
            resultado = self.calcular_pi(num_puntos)
            
            mensaje = {
                "worker": self.id_worker,
                "pi_aproximado": resultado,
                "puntos_usados": num_puntos
            }
            
            self.canal.basic_publish(
                exchange='',
                routing_key=COLA_RESULTADOS,
                body=json.dumps(mensaje),
                properties=pika.BasicProperties(delivery_mode=2)  # Mensaje persistente
            )
            
            print(f"Envié resultado: {resultado}")
            
            canal.basic_ack(delivery_tag=metodo.delivery_tag)
            
        except Exception as error:
            print(f"Ocurrió un error: {error}")
            canal.basic_ack(delivery_tag=metodo.delivery_tag)
    
    def empezar(self):
        self.canal.basic_qos(prefetch_count=1)  # Procesar un mensaje a la vez
        
        self.canal.basic_consume(
            queue=COLA_TAREAS,
            on_message_callback=self.procesar_mensaje
        )
        
        print("Esperando mensajes...")
        self.canal.start_consuming()

if __name__ == "__main__":  # ✓ Corregido: doble guion bajo
    worker = WorkerMontecarlo()
    worker.empezar()