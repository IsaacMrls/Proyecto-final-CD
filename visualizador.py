#!/usr/bin/env python3
"""
Dashboard gráfico para visualizar la simulación distribuida.
"""

import pika
import json
import threading
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

RABBITMQ_HOST = "192.168.1.75"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "Isaac_mrls"
RABBITMQ_PASSWORD = "876435"
RABBITMQ_VHOST = "/"

QUEUE_STATS = "cola_stats_productor"
QUEUE_RESULTS = "cola_resultados"

class SimulationDashboard:
    def __init__(self):
        # Datos en memoria para graficar
        self.results = []
        self.producer_stats = {}
        self.worker_counts = {}
        self.lock = threading.Lock() # Seguridad para hilos

        # Iniciar hilo de consumo RabbitMQ
        self.consumer_thread = threading.Thread(target=self.consumir_mensajes, daemon=True)
        self.consumer_thread.start()

        # Configuración visual
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(10, 8))
        self.fig.suptitle("Monitor de Simulación Montecarlo Distribuida")

    def conectar_rabbitmq(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=RABBITMQ_PORT, 
            virtual_host=RABBITMQ_VHOST, credentials=credentials
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        channel.queue_declare(queue=QUEUE_STATS, durable=False)
        channel.queue_declare(queue=QUEUE_RESULTS, durable=True)
        return channel

    def consumir_mensajes(self):
        channel = self.conectar_rabbitmq()
        print("[Dashboard] Conectado y escuchando métricas...")

        def callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                
                with self.lock:
                    # Si es mensaje de estadísticas del productor
                    if data.get("type") == "stats":
                        self.producer_stats = data
                    
                    # Si es mensaje de resultado de un worker
                    elif data.get("type") == "result":
                        self.results.append(data["result"])
                        w_id = data["worker_id"]
                        self.worker_counts[w_id] = self.worker_counts.get(w_id, 0) + 1
            except Exception as e:
                print(f"Error procesando mensaje: {e}")

        # Consumir de ambas colas
        channel.basic_consume(queue=QUEUE_STATS, on_message_callback=callback, auto_ack=True)
        channel.basic_consume(queue=QUEUE_RESULTS, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()

    def update_plot(self, frame):
        with self.lock:
            # Gráfico 1: Histograma de Resultados
            self.ax1.clear()
            if self.results:
                self.ax1.hist(self.results, bins=50, color='royalblue', alpha=0.7, edgecolor='black')
                self.ax1.set_title(f"Distribución de Resultados (n={len(self.results)})")
                self.ax1.set_ylabel("Frecuencia")
                self.ax1.grid(True, linestyle='--', alpha=0.5)
            else:
                self.ax1.text(0.5, 0.5, "Esperando resultados...", ha='center')

            # Gráfico 2: Carga de Trabajo por Worker + Info Productor
            self.ax2.clear()
            if self.worker_counts:
                workers = list(self.worker_counts.keys())
                counts = list(self.worker_counts.values())
                # Simplificar nombres largos de workers para el gráfico
                labels = [w.split('_')[1] for w in workers] 
                
                bars = self.ax2.bar(labels, counts, color='mediumseagreen')
                self.ax2.bar_label(bars)
                self.ax2.set_title("Escenarios Procesados por Nodo")
            
            # Texto de estadísticas del productor en la esquina
            if self.producer_stats:
                throughput = self.producer_stats.get("throughput_scenarios_per_second", 0)
                msg = f"Velocidad Productor: {throughput:.2f} esc/s"
                self.ax2.text(0.95, 0.90, msg, transform=self.ax2.transAxes, 
                              ha='right', fontsize=10, bbox=dict(facecolor='white', alpha=0.8))

    def run(self):
        ani = FuncAnimation(self.fig, self.update_plot, interval=1000)
        plt.show()

if __name__ == "__main__":
    dash = SimulationDashboard()
    dash.run()