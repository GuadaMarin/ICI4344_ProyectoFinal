# Google Drive Distribuido — ICI4344 Proyecto Final

Sistema distribuido de **almacenamiento y sincronización colaborativa de archivos** inspirado en Google Drive, desarrollado en **Java 17**. El sistema está formado por **tres nodos independientes** que cooperan sin un coordinador central y demuestra los conceptos centrales del curso:

- **Topología multinodo**: 3 nodos (`nodo1`, `nodo2`, `nodo3`), cada uno en su propio proceso/JVM, con un **registro de membresía** (`RegistroMembresia`).
- **Ordenamiento de eventos sin reloj global**: relojes lógicos de **Lamport** (`RelojLamport`) en todos los mensajes.
- **Coordinación distribuida**: exclusión mutua con el algoritmo de **Ricart y Agrawala** (`ServicioExclusionMutua`) para proteger el recurso crítico compartido (el documento que se edita de forma concurrente).
- **Tolerancia a fallos**: detección por **heartbeats** y *timeouts* (`DetectorFallos`), reconfiguración dinámica de la membresía y retransmisión de solicitudes ante pérdida de mensajes.
- **Comunicación segura**: canal de clientes sobre **TLS** y *marshalling* de objetos serializados.
- **Prueba de tráfico**: generador de carga concurrente con métricas (throughput, latencia p95, tasa de error) y falla inducida.

---

## 1. Requisitos

- **Java JDK 17 o superior** (probado con OpenJDK 21). Verifícalo con:
  ```bash
  java -version
  ```
- No se requiere Maven: el proyecto se compila directamente con `javac`. (Igual se incluye un `pom.xml`.)
- Todos los comandos se ejecutan desde la carpeta `Codigo/DriveSimplificado`.

> **Nota sobre TLS:** el repositorio incluye un *keystore* autofirmado (`keystore.jks`) con contraseña `password`. Los nodos y clientes deben ejecutarse **desde la carpeta `Codigo/DriveSimplificado`** para que encuentren ese archivo.

---

## 2. Compilación

Desde `Codigo/DriveSimplificado`:

**Linux / macOS**
```bash
cd Codigo/DriveSimplificado
find src -name "*.java" > sources.txt
javac -d build @sources.txt
```

**Windows (CMD)**
```bat
cd Codigo\DriveSimplificado
dir /s /b src\*.java > sources.txt
javac -d build @sources.txt
```

Las clases compiladas quedan en `build/`.

---

## 3. Ejecución de los nodos

El sistema usa, por cada nodo, un **puerto de clientes** (TLS) y un **puerto de coordinación** entre nodos:

| Nodo  | Puerto clientes | Puerto coordinación |
| :---- | :-------------- | :------------------ |
| nodo1 | 9000            | 9100                |
| nodo2 | 9001            | 9101                |
| nodo3 | 9002            | 9102                |

Abre **tres terminales** (una por nodo), todas situadas en `Codigo/DriveSimplificado`:

```bash
# Terminal 1
java -cp build com.googledrive.storage.server.NodoAlmacenamiento nodo1 9000

# Terminal 2
java -cp build com.googledrive.storage.server.NodoAlmacenamiento nodo2 9001

# Terminal 3
java -cp build com.googledrive.storage.server.NodoAlmacenamiento nodo3 9002
```

Cada nodo debe imprimir algo como:
```
[Heartbeat] nodo1 detector de fallos activo (intervalo 1000 ms)
[Coordinación] nodo1 escuchando en puerto 9100
-> NODO1 iniciado (Puerto clientes: 9000)
```

En el log de cada nodo verás, durante las operaciones, las **marcas de Lamport** y la **exclusión mutua**:
```
[6] ---> Nodo nodo1 ENTRA a (documento_compartido.txt)
[7] <--- Nodo nodo1 SALE de (documento_compartido.txt)
[Ricart-Agrawala] nodo1 DIFIERE a nodo2
[Métricas] nodo1 | mensajes coordinación (Ricart-Agrawala) = ... | reloj Lamport = ... | nodos vivos = [nodo2, nodo3]
```

---

## 4. Clientes de prueba

Con los tres nodos en marcha, en una **cuarta terminal** (también en `Codigo/DriveSimplificado`):

**a) Prueba de almacenamiento (subir un archivo):**
```bash
java -cp build com.googledrive.client.test.ClientePruebaAlmacenamiento
```

**b) Prueba de sincronización colaborativa (3 usuarios editan el mismo documento a la vez):**
```bash
java -cp build com.googledrive.client.sync.ClientePruebaSincronizacion
```
Muestra cómo el sistema serializa las ediciones concurrentes y devuelve el documento consistente.

---

## 5. Prueba de tráfico (carga) y falla inducida

El **generador de carga** lanza muchos hilos concurrentes que ejercitan las dos funciones principales (incluido el recurso crítico compartido) y recolecta métricas.

```bash
# Uso: GeneradorCarga [numHilos] [duracionSegundos]
java -Djava.awt.headless=true -cp build com.googledrive.client.carga.GeneradorCarga 50 60
```

Por defecto usa **50 hilos durante 60 segundos**. Al terminar genera, en la carpeta `resultados_carga/`:

- `resumen_metricas.txt` — throughput, latencia promedio/p50/p95, tasa de error.
- `throughput_por_segundo.csv` — evolución del throughput (útil para ver la falla inducida).
- `grafico_throughput.png` — gráfico del throughput por segundo.

**Para demostrar la tolerancia a fallos (falla inducida):** mientras el generador corre, **cierra la terminal de `nodo2`** (o `Ctrl+C`) alrededor del segundo 30. Observarás que:
1. El detector de *heartbeats* declara muerto a `nodo2` (`nodos vivos = [nodo1, nodo3]`).
2. El throughput cae brevemente (~2 s) y luego se **recupera**.
3. Gracias al *failover* del cliente y a la reconfiguración de la membresía, el servicio **no se detiene**.

> Una corrida de referencia (50 hilos, 60 s, con caída de `nodo2` en el segundo 30) está incluida en `resultados_carga/` como evidencia: ≈ 430 ops/seg, latencia p95 ≈ 149 ms, 0 % de error con *failover*, y recuperación en ~2 s.

---

## 6. Estructura del proyecto

```
Codigo/DriveSimplificado/
├── pom.xml
├── keystore.jks                         # certificado TLS autofirmado (password: "password")
├── src/main/java/com/googledrive/
│   ├── core/
│   │   ├── models/
│   │   │   ├── PeticionArchivo.java      # petición cliente→nodo (con contenido y marca Lamport)
│   │   │   ├── MensajeCoordinacion.java  # mensaje entre nodos (SOLICITUD/PERMISO/HEARTBEAT)
│   │   │   └── RegistroMembresia.java    # registro/descubrimiento de nodos
│   │   └── utils/
│   │       ├── RelojLamport.java         # reloj lógico de Lamport
│   │       └── Utils.java                # checksum MD5
│   └── storage/server/
│       ├── NodoAlmacenamiento.java       # proceso principal de cada nodo (servidor TLS multihilo)
│       ├── WorkerCliente.java            # atiende cada conexión de cliente
│       ├── ServicioExclusionMutua.java   # Ricart-Agrawala + membresía dinámica
│       ├── DetectorFallos.java           # heartbeats / detección de caídas
│       └── GestorArchivosLocal.java      # persistencia con locks lectura/escritura + MD5
└── src/test/java/com/googledrive/client/
    ├── test/ClientePruebaAlmacenamiento.java   # prueba de subida
    ├── sync/ClientePruebaSincronizacion.java   # prueba de edición concurrente
    └── carga/GeneradorCarga.java               # prueba de tráfico (carga) + métricas + gráfico
```

---

## 7. Mapeo con la rúbrica

| Requisito de la rúbrica | Dónde se cumple |
| :---- | :---- |
| 2.1 Topología multinodo (3+ nodos, membresía) | `NodoAlmacenamiento`, `RegistroMembresia` |
| 2.2 Ordenamiento de eventos (Lamport) | `RelojLamport`, marcas en los logs |
| 2.3 Coordinación distribuida (Ricart-Agrawala) | `ServicioExclusionMutua` |
| 2.4 Tolerancia a fallos (heartbeats, recuperación) | `DetectorFallos`, retransmisión y reconfiguración |
| 3 Prueba de tráfico (≥50 hilos, ≥60 s, métricas, falla) | `GeneradorCarga`, `resultados_carga/` |
| 4.4 Distribución y comunicación (sockets, marshalling) | sockets TLS + objetos `Serializable` |
| 4.5 Coordinación y concurrencia (Locks/synchronized) | `ServicioExclusionMutua`, `GestorArchivosLocal` |
| 4.6 Tolerancia a fallos y funciones bajo carga | demostrado en la prueba de tráfico |
