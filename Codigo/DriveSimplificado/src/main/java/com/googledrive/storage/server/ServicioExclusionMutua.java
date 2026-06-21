package com.googledrive.storage.server;

import com.googledrive.core.models.MensajeCoordinacion;
import com.googledrive.core.models.RegistroMembresia;
import com.googledrive.core.utils.RelojLamport;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class ServicioExclusionMutua {
    private final String idNodoLocal;
    private final RelojLamport relojLogico;
    private final DetectorFallos detectorFallos;

    private static final long INTERVALO_HEARTBEAT_MS = 1000;
    private static final int TIMEOUT_CONEXION_MS = 1500;
    private static final int BACKLOG_COORDINACION = 200;
    private static final long INTERVALO_REENVIO_MS = 1200; // retransmisión de solicitudes no respondidas

    public enum EstadoSeccionCritica { 
        LIBERADO,  
        BUSCANDO,  
        OCUPANDO   
    }
    
    private final Map<String, EstadoSeccionCritica> estadosPorArchivo = new ConcurrentHashMap<>();
    private final Map<String, Integer> tiemposDeMisSolicitudes = new ConcurrentHashMap<>();
    private final Map<String, List<MensajeCoordinacion>> solicitudesDiferidas = new ConcurrentHashMap<>();
    private final Map<String, Object> candadosDeEspera = new ConcurrentHashMap<>();

    // Conjunto idempotente de nodos que ya concedieron el permiso para cada archivo.
    private final Map<String, Set<String>> nodosQueConcedieron = new ConcurrentHashMap<>();
    // Nodos a los que se les envió la solicitud y de quienes se espera permiso.
    private final Map<String, Set<String>> destinatariosPorArchivo = new ConcurrentHashMap<>();
    // Candado local por archivo: serializa los hilos del propio nodo que compiten por el mismo recurso.
    private final Map<String, ReentrantLock> candadosLocalesPorArchivo = new ConcurrentHashMap<>();

    private final AtomicLong mensajesCoordinacion = new AtomicLong(0);

    // Pool para atender los mensajes de coordinación entrantes de forma concurrente.
    private final ExecutorService poolCoordinacion = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "coord-worker");
        t.setDaemon(true);
        return t;
    });

    public ServicioExclusionMutua(String idNodoLocal, RelojLamport relojLogico) {
        this.idNodoLocal = idNodoLocal;
        this.relojLogico = relojLogico;
        this.detectorFallos = new DetectorFallos(idNodoLocal, INTERVALO_HEARTBEAT_MS);
    }

    public void iniciarEscuchaCoordinacion() {
        int puertoLocal = RegistroMembresia.obtenerPuerto(idNodoLocal);
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(puertoLocal, BACKLOG_COORDINACION)) {
                System.out.println("[Coordinación] " + idNodoLocal + " escuchando en puerto " + puertoLocal);
                while (true) {
                    Socket conexionExterna = serverSocket.accept();
                    poolCoordinacion.execute(() -> manejarMensajeEntrante(conexionExterna));
                }
            } catch (IOException e) {
                System.err.println("Error en hilo de coordinación: " + e.getMessage());
            }
        }, "coordinacion-" + idNodoLocal).start();

        detectorFallos.iniciar();
    }

    private void manejarMensajeEntrante(Socket socketExterno) {
        try (ObjectInputStream ois = new ObjectInputStream(socketExterno.getInputStream())) {
            MensajeCoordinacion mensaje = (MensajeCoordinacion) ois.readObject();

            detectorFallos.registrarContacto(mensaje.getIdNodoOrigen());

            if (mensaje.getTipo() == MensajeCoordinacion.Tipo.HEARTBEAT) {
                return; // El latido solo sirve para la detección de fallos.
            }

            relojLogico.registrarRecepcion(mensaje.getTiempoLamport());

            if (mensaje.getTipo() == MensajeCoordinacion.Tipo.SOLICITUD_ACCESO) {
                evaluarSolicitudExterna(mensaje);
            } else if (mensaje.getTipo() == MensajeCoordinacion.Tipo.PERMISO_CONCEDIDO) {
                evaluarPermisoRecibido(mensaje);
            }
        } catch (Exception e) {
            System.err.println("Error procesando mensaje de coordinación.");
        } finally {
            try { socketExterno.close(); } catch (IOException e) {}
        }
    }

    // Lógica de Ricart-Agrawala: se decide bajo el monitor y el envío del permiso se hace fuera de él.
    private void evaluarSolicitudExterna(MensajeCoordinacion msjExterno) {
        String archivo = msjExterno.getNombreArchivo();
        boolean conceder;

        synchronized (this) {
            EstadoSeccionCritica estadoActual = estadosPorArchivo.getOrDefault(archivo, EstadoSeccionCritica.LIBERADO);
            boolean deboDiferir = false;
            Integer tiempoDeMiSolicitud = tiemposDeMisSolicitudes.get(archivo);

            if (estadoActual == EstadoSeccionCritica.OCUPANDO) {
                deboDiferir = true;
            } else if (estadoActual == EstadoSeccionCritica.BUSCANDO && tiempoDeMiSolicitud != null) {
                boolean miSolicitudFuePrimero = tiempoDeMiSolicitud < msjExterno.getTiempoLamport();
                boolean huboEmpateGanoYo = (tiempoDeMiSolicitud == msjExterno.getTiempoLamport())
                                            && (idNodoLocal.compareTo(msjExterno.getIdNodoOrigen()) < 0);
                if (miSolicitudFuePrimero || huboEmpateGanoYo) {
                    deboDiferir = true;
                }
            }

            if (deboDiferir) {
                // Se difiere evitando duplicados del mismo origen (las solicitudes pueden retransmitirse).
                List<MensajeCoordinacion> lista = solicitudesDiferidas.computeIfAbsent(archivo, k -> new ArrayList<>());
                boolean yaDiferido = lista.stream().anyMatch(m -> m.getIdNodoOrigen().equals(msjExterno.getIdNodoOrigen()));
                if (!yaDiferido) {
                    lista.add(msjExterno);
                    System.out.println("[Ricart-Agrawala] " + idNodoLocal + " DIFIERE a " + msjExterno.getIdNodoOrigen());
                }
            }
            conceder = !deboDiferir;
        }

        if (conceder) {
            enviarPermiso(msjExterno.getIdNodoOrigen(), archivo);
        }
    }

    private void evaluarPermisoRecibido(MensajeCoordinacion msjPermiso) {
        String archivo = msjPermiso.getNombreArchivo();
        nodosQueConcedieron.computeIfAbsent(archivo, k -> ConcurrentHashMap.newKeySet())
                           .add(msjPermiso.getIdNodoOrigen());

        Object candadoHiloLocal = candadosDeEspera.get(archivo);
        if (candadoHiloLocal != null) {
            synchronized (candadoHiloLocal) {
                candadoHiloLocal.notifyAll();
            }
        }
    }

    // Pide acceso al resto de nodos vivos y bloquea hasta recibir todos los permisos
    public void solicitarAccesoCritico(String archivo) {
        ReentrantLock candadoLocal = candadosLocalesPorArchivo.computeIfAbsent(archivo, k -> new ReentrantLock());
        candadoLocal.lock();

        int tiempoDeMiSolicitud = relojLogico.registrarEnvio();

        Set<String> destinatarios;
        synchronized (this) {
            estadosPorArchivo.put(archivo, EstadoSeccionCritica.BUSCANDO);
            tiemposDeMisSolicitudes.put(archivo, tiempoDeMiSolicitud);
            nodosQueConcedieron.put(archivo, ConcurrentHashMap.newKeySet());
            candadosDeEspera.putIfAbsent(archivo, new Object());
            destinatarios = new HashSet<>(detectorFallos.nodosVivos());
        }

        // Envío inicial de solicitudes (fuera del monitor para no bloquear la coordinación entrante).
        Set<String> enviadosOk = new HashSet<>();
        for (String nodoDestino : destinatarios) {
            if (enviarSolicitud(nodoDestino, archivo, tiempoDeMiSolicitud)) {
                enviadosOk.add(nodoDestino);
            }
        }
        destinatariosPorArchivo.put(archivo, enviadosOk);

        // Espera con retransmisión: si un permiso se pierde (omisión), se reenvía la solicitud a los
        // nodos vivos que aún no han concedido; si un nodo cae, deja de exigirse su permiso.
        Object candado = candadosDeEspera.get(archivo);
        long ultimoReenvio = System.currentTimeMillis();
        while (!tienePermisosDeTodosLosVivos(archivo)) {
            synchronized (candado) {
                if (!tienePermisosDeTodosLosVivos(archivo)) {
                    try {
                        candado.wait(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            if (System.currentTimeMillis() - ultimoReenvio > INTERVALO_REENVIO_MS) {
                reenviarSolicitudesPendientes(archivo, tiempoDeMiSolicitud);
                ultimoReenvio = System.currentTimeMillis();
            }
        }

        synchronized (this) {
            estadosPorArchivo.put(archivo, EstadoSeccionCritica.OCUPANDO);
        }
        System.out.println("[" + relojLogico.obtenerTiempo() + "] ---> Nodo " + idNodoLocal + " ENTRA a (" + archivo + ")");
    }

    private boolean tienePermisosDeTodosLosVivos(String archivo) {
        Set<String> destinatarios = destinatariosPorArchivo.getOrDefault(archivo, Collections.emptySet());
        Set<String> conceden = nodosQueConcedieron.getOrDefault(archivo, Collections.emptySet());
        for (String nodo : destinatarios) {
            if (detectorFallos.estaVivo(nodo) && !conceden.contains(nodo)) {
                return false;
            }
        }
        return true;
    }

    private void reenviarSolicitudesPendientes(String archivo, int tiempoDeMiSolicitud) {
        Set<String> destinatarios = destinatariosPorArchivo.getOrDefault(archivo, Collections.emptySet());
        Set<String> conceden = nodosQueConcedieron.getOrDefault(archivo, Collections.emptySet());
        for (String nodo : destinatarios) {
            if (detectorFallos.estaVivo(nodo) && !conceden.contains(nodo)) {
                enviarSolicitud(nodo, archivo, tiempoDeMiSolicitud);
            }
        }
    }

    public void liberarAccesoCritico(String archivo) {
        List<MensajeCoordinacion> solicitudesAprobadas;
        relojLogico.registrarEventoLocal();
        
        synchronized (this) {
            estadosPorArchivo.put(archivo, EstadoSeccionCritica.LIBERADO);
            tiemposDeMisSolicitudes.remove(archivo);
            solicitudesAprobadas = new ArrayList<>(solicitudesDiferidas.getOrDefault(archivo, new ArrayList<>()));
            solicitudesDiferidas.remove(archivo);
            nodosQueConcedieron.remove(archivo);
            destinatariosPorArchivo.remove(archivo);
        }
        
        System.out.println("[" + relojLogico.obtenerTiempo() + "] <--- Nodo " + idNodoLocal + " SALE de (" + archivo + ")");
        
        for (MensajeCoordinacion peticionPendiente : solicitudesAprobadas) {
            enviarPermiso(peticionPendiente.getIdNodoOrigen(), archivo);
        }

        ReentrantLock candadoLocal = candadosLocalesPorArchivo.get(archivo);
        if (candadoLocal != null && candadoLocal.isHeldByCurrentThread()) {
            candadoLocal.unlock();
        }
    }

    private boolean enviarSolicitud(String nodoDestino, String archivo, int tiempoDeMiSolicitud) {
        return enviarMensajeA(nodoDestino, new MensajeCoordinacion(
                MensajeCoordinacion.Tipo.SOLICITUD_ACCESO, tiempoDeMiSolicitud, idNodoLocal, archivo));
    }

    private void enviarPermiso(String nodoDestino, String archivo) {
        int tiempoPermiso = relojLogico.registrarEnvio();
        enviarMensajeA(nodoDestino, new MensajeCoordinacion(MensajeCoordinacion.Tipo.PERMISO_CONCEDIDO, tiempoPermiso, idNodoLocal, archivo));
    }

    private boolean enviarMensajeA(String nodoDestino, MensajeCoordinacion mensaje) {
        int puertoDestino = RegistroMembresia.obtenerPuerto(nodoDestino);
        if (puertoDestino == -1) return false;
        
        try (Socket socketRed = new Socket()) {
            socketRed.connect(new InetSocketAddress(RegistroMembresia.obtenerHost(), puertoDestino), TIMEOUT_CONEXION_MS);
            ObjectOutputStream flujoSalida = new ObjectOutputStream(socketRed.getOutputStream());
            flujoSalida.writeObject(mensaje);
            flujoSalida.flush();
            mensajesCoordinacion.incrementAndGet();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** Total de mensajes del algoritmo de coordinación (Ricart-Agrawala) enviados por este nodo. */
    public long getMensajesCoordinacion() {
        return mensajesCoordinacion.get();
    }

    public DetectorFallos getDetectorFallos() {
        return detectorFallos;
    }
}
