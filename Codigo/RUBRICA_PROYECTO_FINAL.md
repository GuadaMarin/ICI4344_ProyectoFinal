## 1. Definición del Sistema

El objetivo de este proyecto es construir una aplicación distribuida en Java en la que varios procesos independientes —al menos tres ejecutándose al mismo tiempo— trabajen en conjunto para sostener dos funciones principales de gran escala. La diferencia de fondo con el Proyecto Parcial es que aquí ya no basta con un cliente-servidor: el sistema tiene que coordinarse sin un reloj común y debe seguir prestando servicio aunque uno de sus nodos se caiga.

## Cada grupo debe:

1. Tomar como punto de partida el dominio trabajado en el Parcial (chat, reservas, banca, delivery u otro) y llevarlo a una arquitectura de varios nodos.

2. Dejar en claro cuáles son las dos funciones principales y por qué tiene sentido distribuirlas.

3. Lograr que los procesos remotos —en máquinas distintas o en varias instancias dentro de la red local— se comuniquen de verdad, cuidando la transparencia de acceso y de ubicación.

4. Incorporar y dejar operativos los mecanismos de coordinación de las Unidades 5 y 6 que se piden en la Sección 2.

## Lo que el sistema debe demostrar:

Que está realmente distribuido: tres o más nodos colaborando, sin que la caída de uno solo deje al sistema fuera de servicio.

Que ordena los eventos sin depender de un reloj global, usando relojes de Lamport o vectoriales.

Que los nodos se coordinan entre sí, ya sea para acceder a un recurso en exclusión mutua o para elegir un coordinador.

Que tolera fallos independientes (caída de un nodo y pérdida de mensajes), detectándolos y recuperándose.

Que su comportamiento se puede medir bajo carga, según la prueba de tráfico de la Sección 3.

## 2. Requisitos Técnicos Distribuidos Obligatorios

Sobre lo que ya se exigió en el Parcial, el Final suma los requisitos que siguen. Como mínimo, cada grupo debe resolver los puntos 2.1, 2.2 y 2.4, y al menos uno de los algoritmos del punto 2.3.

## 2.1 Topología multinodo

• Tres nodos como mínimo, cada uno corriendo en su propio proceso o JVM; lo ideal es repartirlos en máquinas distintas de la misma red local.

Arquitectura peer-to-peer o de varios servidores. Queda descartado depender de un único servidor que centralice toda la coordinación.

Los nodos deben conocerse entre sí mediante un registro o un mecanismo de descubrimiento (lista de membresía).

## 2.2 Ordenamiento de eventos (Unidad 5)

• Relojes de Lamport o vectoriales aplicados a los mensajes que intercambian los nodos.

Al menos una de las dos funciones principales debe mostrar un ordenamiento causal correcto: el orden total de los mensajes de un chat, la secuencia de transacciones, etc.

Cada evento relevante debe quedar registrado en un log con su marca lógica o vectorial, para poder mostrar el orden en el informe.

## 2.3 Coordinación distribuida (Unidad 6) — elegir al menos uno

Exclusión mutua distribuida mediante el algoritmo basado en anillo, el de Ricart y Agrawala o la votación de Maekawa, para proteger un recurso crítico compartido.

Elección de coordinador con el algoritmo abusón (Bully) o el basado en anillo, que se dispare solo cuando se detecta que el coordinador actual cayó.

Consenso simplificado (opcional, con puntaje extra): acordar un valor replicado entre los nodos por quórum o con un esquema reducido tipo Raft.

## 2.4 Tolerancia a fallos

Detección mediante heartbeats o timeouts, tanto de nodos caídos (crash) como de mensajes que se pierden (omisión).

Recuperación efectiva: ante una caída, el sistema se reorganiza —vuelve a elegir coordinador, redistribuye la carga o reintegra al nodo— sin que el servicio se detenga por completo.

## 3. Prueba de Tráfico Real (Carga)

El sistema tiene que pasar por una prueba de carga real y repetible. No se aceptan demostraciones con un solo cliente conectado: la idea es ver cómo se comporta de verdad cuando varios procesos lo usan al mismo tiempo.

## 3.1 Generador de carga

Programar en Java un cliente generador de carga que lance peticiones concurrentes: al menos 50 clientes o hilos simultáneos durante un mínimo de 60 segundos sostenidos.

• Esa carga debe ejercitar las dos funciones principales y, en particular, el recurso protegido por exclusión mutua.

## 3.2 Métricas que deben recolectar

Throughput, entendido como la cantidad de peticiones atendidas por segundo.

Latencia, reportando el promedio y el percentil 95 (p95) del tiempo de respuesta.

Cantidad de mensajes que genera el algoritmo de coordinación (exclusión mutua o elección).

• Tasa de error o de pérdida, tanto en régimen normal como durante la caída de un nodo.

## 3.3 Falla inducida durante la prueba

En plena prueba de carga deben derribar a propósito al coordinador o a un nodo activo, y medir cuánto tarda el sistema en recuperarse y cómo se ven afectadas la latencia y los errores.

## 3.4 Evidencia que deben entregar

Una tabla y al menos un gráfico con los resultados de las métricas, dentro del informe.

• Los logs de la corrida (marcas lógicas, eventos de elección y detección de la falla) adjuntos a la entrega.

## 4. Pauta de Evaluación Detallada

## I. Informe Técnico e Inspección de Modelos (35%)

Requisito: El informe debe usar el Formato de Informe de Pregrado de la Escuela, incluida su portada o página de título oficial.

Entrega: Una versión digital en PDF en el aula virtual y una copia impresa el día de la presentación.

## 4.1 Fundamentación y Teoría (10%)

• Explicar cómo aparecen la concurrencia y la ausencia de un reloj global en el sistema que construyeron.

• Justificar por qué eligieron Lamport o relojes vectoriales y qué problema concreto les resuelve.

• Mostrar cómo logran la transparencia de acceso y de ubicación en una topología de varios nodos.

## 4.2 Modelado de Ingeniería (15%)

Modelo físico: los tres o más nodos, el entorno de red (LAN/WAN) y un esquema de cómo se interconectan.

Modelo arquitectónico: capas, componentes, tipo de arquitectura (P2P o multiservidor) y el flujo de coordinación.

Modelado de las funciones: diagramas de secuencia UML de las dos funciones principales y del algoritmo de coordinación, mostrando el paso de mensajes y las marcas de reloj.

## 4.3 Análisis Fundamental y de Carga (10%)

Modelo de seguridad: qué canales quedan expuestos, qué amenazas existen y cómo las mitigan en Java.

Modelo de fallos: clasificación entre crash y omisión, y las estrategias de detección y recuperación que programaron.

Lectura de la prueba de tráfico: qué dicen el throughput, la latencia p95 y los mensajes de coordinación, y cómo respondió el sistema a la falla inducida.

## II. Implementación y Código Fuente en Java (35%)

## 4.4 Distribución y Comunicación (10%)

Topología real de tres o más nodos comunicándose por sockets (TCP o UDP), con marshalling de estructuras de datos complejas.

Mecanismo de descubrimiento o membresía de nodos en funcionamiento.

## 4.5 Coordinación y Ordenamiento (15%)

Relojes de Lamport o vectoriales bien implementados sobre los mensajes.

Un algoritmo de exclusión mutua o de elección de coordinador que funcione y que se pueda verificar.

Concurrencia bien resuelta: servidor multihilo con sincronización correcta (synchronized o Locks) en las regiones críticas.

## 4.6 Tolerancia a Fallos y Lógica de Funciones (10%)

Las dos funciones principales operan correctamente incluso bajo carga.

• El sistema detecta caídas (por heartbeats o timeouts) y se recupera sin venirse abajo; el manejo de excepciones de red está cubierto.

## III. Presentación, Demo y Prueba de Tráfico en Vivo (30%)

Requisito: La presentación tiene que ser coherente con el informe y con el código entregado.

## 4.7 Exposición y Video (15%)

Deben exponer al menos tres integrantes.

Explicar con claridad los objetivos, los modelos (físico, arquitectónico y UML) y su relación con lo implementado.

Video demostrativo de no más de 3 minutos, mostrando la ejecución con varios nodos y las funciones principales.

## 4.8 Demo de Carga y Falla en Vivo (10%)

Correr en vivo la prueba de tráfico (carga concurrente) sobre tres o más nodos.

Derribar en vivo un nodo o al coordinador y mostrar la re-elección o la recuperación junto con las métricas que resultan.

## 4.9 Defensa Técnica (5%)

• Responder las preguntas del profesor y de los compañeros sobre el diseño, los algoritmos de coordinación y las decisiones de código.

Sostener una coherencia total entre el informe, el código y la presentación.

## 5. Entregables Obligatorios

## En el aula virtual (digital):

5. Informe técnico en PDF, con el formato y la portada de pregrado de la Escuela.

6. El proyecto de código fuente completo en un .zip, incluyendo el generador de carga.

7. Los logs de la prueba de tráfico y de la falla inducida.

8. La presentación, con el video demostrativo incluido o adjunto.

## En la sala (físico):

9. El informe impreso, para revisarlo durante la exposición.

## 6. Parámetros de Grupo y Tiempos

Integrantes: Grupos de 5 a 6 personas.

• Tiempo por grupo: 15 minutos de presentación y 5 minutos de preguntas.

## 7. Condiciones de Entrega y Sanciones

• La entrega atrasada se acepta, pero con penalización: se descuenta 1.0 punto de la nota final por cada día de atraso.

Si el informe no respeta el formato exigido —el Formato de Informe de Pregrado de la Escuela, con su página de título—, se descuentan 0.5 puntos (cinco décimas) de la nota del informe.

• Ambos descuentos son independientes y se aplican por separado sobre la nota correspondiente.

## 8. Condición Obligatoria de Presentación

Todos los grupos se presentan en la fecha y hora que se les asigne.

• La presentación es obligatoria y no depende de si entregaron el código o el informe.

Si un grupo no asiste o no presenta, la nota de presentación es automáticamente un 1.0.

## 9. Consideración Final

La evaluación se hace con criterio de ingeniería: lo que importa es que haya coherencia entre lo que se diseñó (los modelos), lo que se documentó (el informe) y lo que efectivamente corre (el código y la prueba de tráfico). En particular, el proyecto debe dejar en evidencia los conceptos centrales del curso: ordenar eventos sin un reloj global, coordinar y poner de acuerdo a los nodos, y tolerar fallos en un sistema realmente distribuido.

## 10. Matriz de Valoración Detallada (Rúbrica de Ingeniería)

## I. Informe Técnico e Inspección de Modelos (35%)

<table><tr><td rowspan=1 colspan=1>Criterio</td><td rowspan=1 colspan=1>No Logrado (0%)</td><td rowspan=1 colspan=1>Algo Logrado (33%)</td><td rowspan=1 colspan=1>Bien Logrado (66%)</td><td rowspan=1 colspan=1>Excelente (100%)</td></tr><tr><td rowspan=1 colspan=1>4.1 Teoría yFundamentos</td><td rowspan=1 colspan=1>No define los conceptos olos copia de la teoria sinaplicarlos al proyecto.</td><td rowspan=1 colspan=1>Menciona los conceptospero no explica cómo semanifiestan en susoftware.</td><td rowspan=1 colspan=1>Explica la mayoria de losconceptos (concurrencia,reloj lógico, fallos,transparencia) y losvincula al código.</td><td rowspan=1 colspan=1>Vinculación técnicacompleta: con ejemplosdel código explica laausencia de reloj global,el ordenamiento deeventos y lastransparencias logradas.</td></tr><tr><td rowspan=1 colspan=1>4.2 Modelado deIngeniería</td><td rowspan=1 colspan=1>Faltan los modelos o nocorresponden al sistemadesarrollado.</td><td rowspan=1 colspan=1>Hay modelos, pero conerrores graves denotación o falta alguno(fisico, arquitectónico 0UML).</td><td rowspan=1 colspan=1>Están los tres modelos,coherentes con el sistemamultinodo, con detallesmenores en la notaciónUML.</td><td rowspan=1 colspan=1>Modelos profesionales ycoherentes: el fisico, elde capas y el desecuencia reflejan porcompleto la topologia yla coordinación.</td></tr><tr><td rowspan=1 colspan=1>4.3 Análisis y Carga</td><td rowspan=1 colspan=1>No hay análisis deseguridad, de fallos ni dela prueba de tráfico.</td><td rowspan=1 colspan=1>Identifica fallos yamenazas de formagenérica y muestramétricas sininterpretarlas.</td><td rowspan=1 colspan=1>Identifica canalesinseguros y fallos (crash,omisión) e interpreta lasmétricas de carga deforma básica.</td><td rowspan=1 colspan=1>Análisis profundo: matrizde fallos con protocolosde recuperación einterpretación completadel throughput, lalatencia p95 y la fallainducida.</td></tr></table>

## II. Implementación y Código Fuente en Java (35%)

<table><tr><td rowspan=1 colspan=1>Criterio</td><td rowspan=1 colspan=1>No Logrado (0%)</td><td rowspan=1 colspan=1>Algo Logrado (33%)</td><td rowspan=1 colspan=1>Bien Logrado (66%)</td><td rowspan=1 colspan=1>Excelente (100%)</td></tr><tr><td rowspan=1 colspan=1>4.4 Distribución yComunicación</td><td rowspan=1 colspan=1>No hay comunicaciónentre procesosindependientes, o soloexiste un nodo.</td><td rowspan=1 colspan=1>Hay sockets, pero soloentre dos nodos oenviando texto simple sinestructuras complejas.</td><td rowspan=1 colspan=1>Topologia de tres o másnodos con sockets ymarshalling funcional deobjetos o estructuras.</td><td rowspan=1 colspan=1>Distribución robusta: treso más nodos condescubrimiento demembresia yserialización avanzada deestados complejos.</td></tr><tr><td rowspan=1 colspan=1>4.5 Coordinación yOrdenamiento</td><td rowspan=1 colspan=1>No implementa relojeslógicos ni algoritmos decoordinación.</td><td rowspan=1 colspan=1>Implementa relojeslógicos o un algoritmo decoordinación, pero conerrores que impidenverificarlo.</td><td rowspan=1 colspan=1>Relojes (Lamport 0vectoriales) y unalgoritmo decoordinaciónfuncionando, conaspectos mejorables.</td><td rowspan=1 colspan=1>Ordenamiento causalcorrecto y exclusiónmutua o elección decoordinador verificablesy bien sincronizados.</td></tr><tr><td rowspan=1 colspan=1>4.6 Tolerancia a Fallosy Funciones</td><td rowspan=1 colspan=1>Las funciones no operano el sistema cae porcompleto ante un fallo.</td><td rowspan=1 colspan=1>Solo opera una función, oel sistema no detecta lacaida de los nodos.</td><td rowspan=1 colspan=1>Ambas funciones operan;detecta los fallos, pero larecuperación es parcial.</td><td rowspan=1 colspan=1>Funcionalidad completabajo carga: detección porheartbeats y recuperación(re-elección oreconfiguración) sincaida total.</td></tr></table>

## III. Presentación, Demo y Prueba de Tráfico (30%)

<table><tr><td rowspan=1 colspan=1>Criterio</td><td rowspan=1 colspan=1>No Logrado (0%)</td><td rowspan=1 colspan=1>Algo Logrado (33%)</td><td rowspan=1 colspan=1>Bien Logrado (66%)</td><td rowspan=1 colspan=1>Excelente (100%)</td></tr><tr><td rowspan=1 colspan=1>4.7 Exposición y Video</td><td rowspan=1 colspan=1>No hay video, la demo nofunciona o exponenmenos de tres integrantes.</td><td rowspan=1 colspan=1>El video pasa de 3minutos o la demo falla;la explicación de losmodelos es superficial.</td><td rowspan=1 colspan=1>Video correcto y demomultinodo funcional. Seexplica la relación entremodelos y código.Exponen tres o másintegrantes.</td><td rowspan=1 colspan=1>Presentación impecable:video sintético (menos de3 minutos), demomultinodo en red real ydominio completo delescenario.</td></tr><tr><td rowspan=1 colspan=1>4.8 Demo de Carga yFalla</td><td rowspan=1 colspan=1>No se ejecuta la pruebade trafico ni la fallainducida.</td><td rowspan=1 colspan=1>Ejecuta carga con pocosclientes o no logramostrar la recuperaciónante la falla.</td><td rowspan=1 colspan=1>Prueba de cargaconcurrente funcional ycaida inducida mostrada,con métricas básicas.</td><td rowspan=1 colspan=1>Prueba de tráfico realexitosa, con métricas envivo y una recuperaciónclara ante la caida delcoordinador o de un</td></tr></table>

Profesor: Francisco Andrés Muñoz Alarcon –francisco.munoz@pucv.cl

<table><tr><td rowspan=1 colspan=1></td><td rowspan=1 colspan=1></td><td rowspan=1 colspan=1></td><td rowspan=1 colspan=1></td><td rowspan=1 colspan=1>nodo.</td></tr><tr><td rowspan=1 colspan=1>4.9 Defensa Técnica</td><td rowspan=1 colspan=1>No logran explicar elcódigo o no respondenlas preguntas.</td><td rowspan=1 colspan=1>Responden con dudas ose contradicen con lo quedice el informe.</td><td rowspan=1 colspan=1>Responden correctamentela mayoria de laspreguntas técnicas.</td><td rowspan=1 colspan=1>Dominio total: justificancon argumentos dearquitectura cadadecisión de diseño y cadaalgoritmo decoordinación.</td></tr></table>