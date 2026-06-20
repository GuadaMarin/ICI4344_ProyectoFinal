package com.googledrive.storage.server;

import com.googledrive.core.models.MensajeCoordinacion;
import com.googledrive.core.models.RegistroMembresia;
import com.googledrive.core.utils.CanalCoordinacion;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Membresía dinámica basada en heartbeats.
 *
 * Cada nodo envía periódicamente un LATIDO a todos los demás nodos conocidos
 * y mantiene el instante del último latido recibido de cada uno. Si un nodo
 * deja de latir por más de {@link #TIMEOUT_MS}, se marca como caído (crash).
 * Esta vista de "nodos vivos" es la que consume la exclusión mutua para
 * calcular el quórum de permisos, evitando que la caída de un nodo bloquee
 * el servicio.
 */
public class ServicioMembresia {

    private static final long INTERVALO_LATIDO_MS = 1500;
    private static final long TIMEOUT_MS = 5000;

    private final String idNodoLocal;
    private final Map<String, Long> ultimoLatido = new ConcurrentHashMap<>();
    private final Map<String, Boolean> estadoVivo = new ConcurrentHashMap<>();

    public ServicioMembresia(String idNodoLocal) {
        this.idNodoLocal = idNodoLocal;
        long ahora = System.currentTimeMillis();
        // Periodo de gracia inicial: asumimos vivos hasta que venza el timeout.
        for (String nodo : RegistroMembresia.obtenerNodos().keySet()) {
            if (!nodo.equals(idNodoLocal)) {
                ultimoLatido.put(nodo, ahora);
                estadoVivo.put(nodo, Boolean.TRUE);
            }
        }
    }

    public void iniciar() {
        Thread hilo = new Thread(this::buclePrincipal, "membresia-" + idNodoLocal);
        hilo.setDaemon(true);
        hilo.start();
    }

    /** Invocado por el listener de coordinación al recibir un LATIDO. */
    public void registrarLatido(String idNodoOrigen) {
        ultimoLatido.put(idNodoOrigen, System.currentTimeMillis());
        Boolean previo = estadoVivo.put(idNodoOrigen, Boolean.TRUE);
        if (previo != null && !previo) {
            System.out.println("[Membresía] " + idNodoOrigen + " REINTEGRADO al clúster.");
        }
    }

    private void buclePrincipal() {
        while (true) {
            enviarLatidos();
            detectarCaidos();
            try {
                Thread.sleep(INTERVALO_LATIDO_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void enviarLatidos() {
        for (String nodo : RegistroMembresia.obtenerNodos().keySet()) {
            if (!nodo.equals(idNodoLocal)) {
                CanalCoordinacion.enviar(nodo,
                        new MensajeCoordinacion(MensajeCoordinacion.Tipo.LATIDO, 0, idNodoLocal, null));
            }
        }
    }

    private void detectarCaidos() {
        long ahora = System.currentTimeMillis();
        for (Map.Entry<String, Long> entrada : ultimoLatido.entrySet()) {
            String nodo = entrada.getKey();
            boolean vivoAhora = (ahora - entrada.getValue()) < TIMEOUT_MS;
            Boolean previo = estadoVivo.put(nodo, vivoAhora);
            if (previo != null && previo && !vivoAhora) {
                System.out.println("[Membresía] " + nodo + " DETECTADO CAÍDO (sin latidos > "
                        + TIMEOUT_MS + " ms). Reconfigurando quórum.");
            }
        }
    }

    public boolean estaVivo(String idNodo) {
        if (idNodo.equals(idNodoLocal)) {
            return true;
        }
        Boolean vivo = estadoVivo.get(idNodo);
        return vivo != null && vivo;
    }

    /** Conjunto de nodos vivos distintos del local (peers a coordinar). */
    public Set<String> obtenerPeersVivos() {
        Set<String> vivos = new HashSet<>();
        for (Map.Entry<String, Boolean> entrada : estadoVivo.entrySet()) {
            if (entrada.getValue()) {
                vivos.add(entrada.getKey());
            }
        }
        return vivos;
    }
}
