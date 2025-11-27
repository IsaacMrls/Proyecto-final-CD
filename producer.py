#!/usr/bin/env python3
"""
Productor de escenarios para simulación Montecarlo distribuida.

- Lee un modelo desde un archivo JSON.
- Publica el modelo en una cola específica (cola_modelo).
- Genera escenarios aleatorios según las distribuciones del modelo.
- Publica cada escenario en la cola de escenarios (cola_escenarios).
- (Opcional) publica estadísticas en una cola de estadísticas (cola_stats_productor).

Formato esperado del archivo JSON de modelo:

{
  "model_id": "modelo_ejemplo_1",
  "description": "Modelo lineal simple",
  "expression": "a * x + b * y",
  "variables": {
    "a": {"dist": "uniform", "min": 1, "max": 5},
    "x": {"dist": "normal", "mu": 10, "sigma": 2},
    "b": {"dist": "uniform", "min": 0, "max": 1},
    "y": {"dist": "exponential", "lambda": 0.5}
  }
}
"""

import json
import time
import random
import argparse
from dataclasses import dataclass, field
from typing import Dict, Any

import pika



RABBITMQ_HOST = "192.168.1.75Ñ"
RABBITMQ_PORT = 5672          
RABBITMQ_USER = "Isaac_mrls"  
RABBITMQ_PASSWORD = "876435"
RABBITMQ_VHOST = "/"          



QUEUE_MODEL = "cola_modelo"
QUEUE_SCENARIOS = "cola_escenarios"
QUEUE_STATS = "cola_stats_productor"


# Clases de dominio

@dataclass
class VariableDistribution:
    """Representa la distribución de probabilidad de una variable."""
    name: str
    params: Dict[str, Any]

    def sample(self) -> float:
        dist_type = self.params.get("dist")

        if dist_type == "uniform":
            a = float(self.params.get("min", 0.0))
            b = float(self.params.get("max", 1.0))
            return random.uniform(a, b)

        elif dist_type == "normal":
            mu = float(self.params.get("mu", 0.0))
            sigma = float(self.params.get("sigma", 1.0))
            return random.gauss(mu, sigma)

        elif dist_type == "exponential":
            lamb = float(self.params.get("lambda", 1.0))
            # random.expovariate usa 1/lambda como media
            return random.expovariate(lamb)

        else:
            raise ValueError(f"Tipo de distribución no soportado: {dist_type}")


@dataclass
class MonteCarloModel:
    """Modelo de simulación Montecarlo."""
    model_id: str
    description: str
    expression: str
    variables: Dict[str, VariableDistribution] = field(default_factory=dict)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "MonteCarloModel":
        model_id = data.get("model_id", "modelo_sin_id")
        description = data.get("description", "")
        expression = data["expression"]

        vars_dict = {}
        raw_vars = data.get("variables", {})
        for name, params in raw_vars.items():
            vars_dict[name] = VariableDistribution(name=name, params=params)

        return MonteCarloModel(
            model_id=model_id,
            description=description,
            expression=expression,
            variables=vars_dict
        )

    @staticmethod
    def from_json_file(path: str) -> "MonteCarloModel":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return MonteCarloModel.from_dict(data)


class ScenarioGenerator:
    """Genera escenarios aleatorios a partir de un modelo."""
    def __init__(self, model: MonteCarloModel):
        self.model = model
        self.scenario_counter = 0

    def generate_scenario(self) -> Dict[str, Any]:
        self.scenario_counter += 1
        scenario_id = self.scenario_counter

        variables_values = {}
        for name, var_dist in self.model.variables.items():
            variables_values[name] = var_dist.sample()

        scenario = {
            "model_id": self.model.model_id,
            "scenario_id": scenario_id,
            "variables": variables_values,
        }
        return scenario


#Cliente RabbitMQ

class RabbitMQClient:
    """Encapsula la conexión y publicación de mensajes en RabbitMQ."""
    def __init__(
        self,
        host: str = RABBITMQ_HOST,
        port: int = RABBITMQ_PORT,
        user: str = RABBITMQ_USER,
        password: str = RABBITMQ_PASSWORD,
        vhost: str = RABBITMQ_VHOST,
    ):
        self.host = host
        self.port = port          
        self.user = user
        self.password = password
        self.vhost = vhost       
        self.connection = None
        self.channel = None

    def connect(self):
        print(f"[RabbitMQClient] Conectando a {self.host}:{self.port}, vhost='{self.vhost}' como '{self.user}'")

        credentials = pika.PlainCredentials(self.user, self.password)
        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,           
            virtual_host=self.vhost,  
            credentials=credentials,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        # Declaración de colas
        self.channel.queue_declare(queue=QUEUE_MODEL, durable=True)
        self.channel.queue_declare(queue=QUEUE_SCENARIOS, durable=True)
        self.channel.queue_declare(queue=QUEUE_STATS, durable=False)

    def publish(self, queue_name: str, message: Dict[str, Any], persistent: bool = True):
        if self.channel is None:
            raise RuntimeError("RabbitMQClient no está conectado.")

        body = json.dumps(message).encode("utf-8")
        properties = pika.BasicProperties(
            delivery_mode=2 if persistent else 1  
        )

        self.channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=properties
        )

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.channel = None



# Productor principal 

class MonteCarloProducer:
    """Orquesta la carga del modelo y la generación/publicación de escenarios."""
    def __init__(self, model_path: str, num_scenarios: int,
                 delay: float = 0.0, stats_interval: int = 100):
        self.model_path = model_path
        self.num_scenarios = num_scenarios
        self.delay = delay
        self.stats_interval = stats_interval

        self.model: MonteCarloModel | None = None
        self.generator: ScenarioGenerator | None = None
        self.client = RabbitMQClient()

    def load_model(self):
        print(f"[Producer] Cargando modelo desde {self.model_path}...")
        self.model = MonteCarloModel.from_json_file(self.model_path)
        self.generator = ScenarioGenerator(self.model)
        print(f"[Producer] Modelo '{self.model.model_id}' cargado.")

    def publish_model(self):
        """Publica el modelo en la cola de modelo."""
        if self.model is None:
            raise RuntimeError("El modelo no está cargado.")

        print("[Producer] Publicando modelo en la cola_modelo...")
        message = {
            "type": "model",
            "model_id": self.model.model_id,
            "description": self.model.description,
            "expression": self.model.expression,
            "variables": {
                name: dist.params for name, dist in self.model.variables.items()
            },
            "timestamp": time.time()
        }

        
        self.client.publish(QUEUE_MODEL, message, persistent=True)
        print("[Producer] Modelo publicado.")

    def publish_stats(self, scenarios_sent: int, start_time: float):
        elapsed = time.time() - start_time
        throughput = scenarios_sent / elapsed if elapsed > 0 else 0

        stats = {
            "type": "stats",
            "model_id": self.model.model_id if self.model else None,
            "scenarios_sent": scenarios_sent,
            "elapsed_seconds": elapsed,
            "throughput_scenarios_per_second": throughput,
            "timestamp": time.time()
        }
        self.client.publish(QUEUE_STATS, stats, persistent=False)
        print(f"[Producer] Stats -> escenarios: {scenarios_sent}, "
              f"velocidad: {throughput:.2f} esc/s")

    def run(self):
        self.client.connect()
        try:
            self.load_model()
            self.publish_model()

            assert self.generator is not None

            print(f"[Producer] Generando {self.num_scenarios} escenarios...")
            start_time = time.time()

            for i in range(1, self.num_scenarios + 1):
                scenario = self.generator.generate_scenario()
                self.client.publish(QUEUE_SCENARIOS, scenario, persistent=True)

                if self.delay > 0:
                    time.sleep(self.delay)

                if i % self.stats_interval == 0:
                    self.publish_stats(i, start_time)

            # Estadísticas finales
            self.publish_stats(self.num_scenarios, start_time)
            print("[Producer] Simulación completada.")

        finally:
            self.client.close()


#CLI

def main():
    parser = argparse.ArgumentParser(
        description="Productor de escenarios Montecarlo con RabbitMQ."
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Ruta al archivo JSON del modelo."
    )
    parser.add_argument(
        "--num-scenarios",
        type=int,
        default=1000,
        help="Número total de escenarios a generar."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Retraso (segundos) entre cada escenario (por defecto 0)."
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=100,
        help="Cada cuántos escenarios enviar estadísticas."
    )

    args = parser.parse_args()

    producer = MonteCarloProducer(
        model_path=args.model,
        num_scenarios=args.num_scenarios,
        delay=args.delay,
        stats_interval=args.stats_interval
    )
    producer.run()

if __name__ == "__main__":
    main()
