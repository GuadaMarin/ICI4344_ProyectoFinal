# DriveSimplificado — Sistema distribuido (Proyecto Final ICI4344)

Contraseña del keystore (TLS): `password`

## Compilación

Desde `Codigo/DriveSimplificado/`:

```
javac -d target/classes $(find src/main/java -name "*.java")
javac -cp target/classes -d target/test-classes $(find src/test/java -name "*.java")
```

## Ejecución de los nodos

Cada nodo es un proceso/JVM independiente. Lanzar los tres desde el mismo
directorio (`Codigo/DriveSimplificado/`) para que compartan `keystore.jks` y
el directorio `storage_data/`:

```
java -cp target/classes com.googledrive.storage.server.NodoAlmacenamiento nodo1 9000
java -cp target/classes com.googledrive.storage.server.NodoAlmacenamiento nodo2 9001
java -cp target/classes com.googledrive.storage.server.NodoAlmacenamiento nodo3 9002
```

Cada nodo abre dos puertos:
- Puerto de clientes (TLS): el indicado por argumento (9000/9001/9002).
- Puerto de coordinación (TCP): 9100/9101/9102 (definidos en `RegistroMembresia`),
  usado por la exclusión mutua Ricart-Agrawala y por los heartbeats de membresía.

## Prueba de carga (Sección 3)

Genera ≥50 clientes concurrentes durante ≥60 s, ejercita subir/editar/descargar
(con foco en el recurso bajo exclusión mutua) y reporta throughput, latencia
promedio, latencia p95 y tasa de error:

```
java -cp target/classes:target/test-classes com.googledrive.client.carga.GeneradorDeCarga 50 60
```

Con falla inducida del `nodo2` a los 30 s (para medir recuperación):

```
java -cp target/classes:target/test-classes com.googledrive.client.carga.GeneradorDeCarga 50 60 nodo2 30
```

## Pruebas funcionales simples

```
java -cp target/classes:target/test-classes com.googledrive.client.test.ClientePruebaAlmacenamiento
java -cp target/classes:target/test-classes com.googledrive.client.sync.ClientePruebaSincronizacion
```
