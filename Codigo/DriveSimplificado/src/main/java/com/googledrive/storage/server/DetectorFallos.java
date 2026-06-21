package com.googledrive.storage.server;

import com.googledrive.core.models.MensajeCoordinacion;
import com.googledrive.core.models.RegistroMembresia;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Detector de fallos basado en heartbeats. Cada nodo envía periódicamente un
 * latido a sus pares por el puerto de coordinación y mantiene, a partir del
 * éxito de esos envíos y de los mensajes recibidos, el conjunto de nodos vivos.
 *
 * Permite la reconfiguración dinámica de la membresía: cuando un par deja de
 * responder se le marca como caído, de modo que el algoritmo de exclusión mutua
 * deja de esperar permisos suyos y el servicio no se detiene.
 */
public class DetectorFallos {

    private final String idNodoLocal;
    private final long intervaloMs;
    private final long umbralCaidaMs;

    // Marca de tiempo del último contacto exitoso con cada nodo par.
    private final ConcurrentHashMap<String, Long> ultimoContactoOk = new ConcurrentHashMap<>();

    // Contador de latidos enviados (separado de los mensajes del algoritmo de coordinación).
    private final AtomicLong heartbeatsEnviados = new AtomicLong(0);

    private final ScheduledExecutorService planificador;

    public DetectorFallos(String idNodoLocal, long intervaloMs) {
        this.idNodoLocal = idNodoLocal;
        this.intervaloMs = intervaloMs;
        // Se considera caído un nodo que no responde tras 3 intervalos.
        this.umbralCaidaMs = intervaloMs * 3;
        this.planificador = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "detector-fallos-" + idNodoLocal);
            t.setDaemon(true);
            return t;
        });
    }

    public void iniciar() {
        planificador.scheduleAtFixedRate(this::rondaHeartbeats, 0, intervaloMs, TimeUnit.MILLISECONDS);
        System.out.println("[Heartbeat] " + idNodoLocal + " detector de fallos activo (intervalo " + intervaloMs + " ms)");
    }

    private void rondaHeartbeats() {
        for (String nodoDestino : RegistroMembresia.obtenerNodos().keySet()) {
            if (!nodoDestino.equals(idNodoLocal)) {
                enviarHeartbeat(nodoDestino);
            }
        }
    }

    private void enviarHeartbeat(String nodoDestino) {
        int puertoDestino = RegistroMembresia.obtenerPuerto(nodoDestino);
        if (puertoDestino == -1) {
            return;
        }
        try (Socket socket = new Socket(RegistroMembresia.obtenerHost(), puertoDestino);
             ObjectOutputStream salida = new ObjectOutputStream(socket.getOutputStream())) {
            salida.writeObject(new MensajeCoordinacion(MensajeCoordinacion.Tipo.HEARTBEAT, 0, idNodoLocal, ""));
            salida.flush();
            heartbeatsEnviados.incrementAndGet();
            registrarContacto(nodoDestino);
        } catch (Exception e) {
            // Silencioso: la ausencia de contacto se traduce, por timeout, en nodo caído.
        }
    }

    /** Registra que se recibió o se intercambió un mensaje con el nodo indicado. */
    public void registrarContacto(String idNodo) {
        if (idNodo != null && !idNodo.isEmpty() && !idNodo.equals(idNodoLocal)) {
            ultimoContactoOk.put(idNodo, System.currentTimeMillis());
        }
    }

    public boolean estaVivo(String idNodo) {
        Long ultimo = ultimoContactoOk.get(idNodo);
        return ultimo != null && (System.currentTimeMillis() - ultimo) <= umbralCaidaMs;
    }

    /** Conjunto de nodos pares (sin contar el local) considerados vivos. */
    public Set<String> nodosVivos() {
        Set<String> vivos = new HashSet<>();
        for (String nodo : RegistroMembresia.obtenerNodos().keySet()) {
            if (!nodo.equals(idNodoLocal) && estaVivo(nodo)) {
                vivos.add(nodo);
            }
        }
        return vivos;
    }

    public long getHeartbeatsEnviados() {
        return heartbeatsEnviados.get();
    }
}
