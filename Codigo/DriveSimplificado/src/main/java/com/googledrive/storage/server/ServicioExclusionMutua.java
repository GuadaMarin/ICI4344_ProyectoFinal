package com.googledrive.storage.server;

import com.googledrive.core.models.MensajeCoordinacion;
import com.googledrive.core.models.RegistroMembresia;
import com.googledrive.core.utils.CanalCoordinacion;
import com.googledrive.core.utils.RelojLamport;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class ServicioExclusionMutua {
    private final String idNodoLocal;
    private final RelojLamport relojLogico;
    private final ServicioMembresia membresia;

    public enum EstadoSeccionCritica {
        LIBERADO,
        BUSCANDO,
        OCUPANDO
    }

    private final Map<String, EstadoSeccionCritica> estadosPorArchivo = new ConcurrentHashMap<>();
    private final Map<String, Integer> tiemposDeMisSolicitudes = new ConcurrentHashMap<>();
    private final Map<String, List<MensajeCoordinacion>> solicitudesDiferidas = new ConcurrentHashMap<>();
    private final Map<String, Integer> permisosRecibidos = new ConcurrentHashMap<>();
    private final Map<String, Object> candadosDeEspera = new ConcurrentHashMap<>();

    // Conjunto de peers a los que se les pidió permiso para este archivo.
    private final Map<String, Set<String>> peersContactados = new ConcurrentHashMap<>();
    // Candado local por archivo: garantiza UNA sola solicitud R-A en vuelo por
    // nodo y archivo, que es justamente el invariante que asume Ricart-Agrawala.
    private final Map<String, ReentrantLock> candadoLocalPorArchivo = new ConcurrentHashMap<>();

    // Métricas: cantidad de mensajes generados por el algoritmo de coordinación.
    private final AtomicLong mensajesEnviados = new AtomicLong(0);
    private final AtomicLong mensajesRecibidos = new AtomicLong(0);

    public ServicioExclusionMutua(String idNodoLocal, RelojLamport relojLogico, ServicioMembresia membresia) {
        this.idNodoLocal = idNodoLocal;
        this.relojLogico = relojLogico;
        this.membresia = membresia;
    }

    // Inicia hilo para escuchar las solicitudes de otros nodos
    public void iniciarEscuchaCoordinacion() {
        int puertoLocal = RegistroMembresia.obtenerPuerto(idNodoLocal);
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(puertoLocal)) {
                System.out.println("[Coordinación] " + idNodoLocal + " escuchando en puerto " + puertoLocal);
                while (true) {
                    Socket conexionExterna = serverSocket.accept();
                    manejarMensajeEntrante(conexionExterna);
                }
            } catch (IOException e) {
                System.err.println("Error en hilo de coordinación: " + e.getMessage());
            }
        }, "coordinacion-" + idNodoLocal).start();
    }

    private void manejarMensajeEntrante(Socket socketExterno) {
        try (ObjectInputStream ois = new ObjectInputStream(socketExterno.getInputStream())) {
            MensajeCoordinacion mensaje = (MensajeCoordinacion) ois.readObject();
            MensajeCoordinacion.Tipo tipo = mensaje.getTipo();

            // Los latidos no son eventos causales del dominio: no avanzan el reloj.
            if (tipo == MensajeCoordinacion.Tipo.LATIDO) {
                membresia.registrarLatido(mensaje.getIdNodoOrigen());
                return;
            }

            if (tipo == MensajeCoordinacion.Tipo.DERRIBAR) {
                System.err.println("[FALLA INDUCIDA] " + idNodoLocal
                        + " recibió orden de derribo. Simulando crash (System.exit).");
                try {
                    socketExterno.close();
                } catch (IOException ignored) {
                }
                System.exit(1);
            }

            mensajesRecibidos.incrementAndGet();
            relojLogico.registrarRecepcion(mensaje.getTiempoLamport());

            if (tipo == MensajeCoordinacion.Tipo.SOLICITUD_ACCESO) {
                evaluarSolicitudExterna(mensaje);
            } else if (tipo == MensajeCoordinacion.Tipo.PERMISO_CONCEDIDO) {
                evaluarPermisoRecibido(mensaje);
            }
        } catch (Exception e) {
            System.err.println("[Coordinación] Error procesando mensaje entrante: " + e.getMessage());
        } finally {
            try {
                socketExterno.close();
            } catch (IOException ignored) {
            }
        }
    }

    // Lógica de Ricart-Agrawala para decidir si dar permiso o diferir la petición
    private synchronized void evaluarSolicitudExterna(MensajeCoordinacion msjExterno) {
        String archivoSolicitado = msjExterno.getNombreArchivo();
        EstadoSeccionCritica estadoActual = estadosPorArchivo.getOrDefault(archivoSolicitado, EstadoSeccionCritica.LIBERADO);

        boolean deboDiferirAlOtro = false;
        Integer tiempoDeMiSolicitud = tiemposDeMisSolicitudes.get(archivoSolicitado);

        // Diferir si ya tenemos el archivo, o si lo pedimos antes (menor timestamp o desempate por nombre)
        if (estadoActual == EstadoSeccionCritica.OCUPANDO) {
            deboDiferirAlOtro = true;
        } else if (estadoActual == EstadoSeccionCritica.BUSCANDO && tiempoDeMiSolicitud != null) {
            boolean miSolicitudFuePrimero = tiempoDeMiSolicitud < msjExterno.getTiempoLamport();
            boolean huboEmpateGanoYo = (tiempoDeMiSolicitud == msjExterno.getTiempoLamport())
                                        && (idNodoLocal.compareTo(msjExterno.getIdNodoOrigen()) < 0);

            if (miSolicitudFuePrimero || huboEmpateGanoYo) {
                deboDiferirAlOtro = true;
            }
        }

        if (deboDiferirAlOtro) {
            solicitudesDiferidas.computeIfAbsent(archivoSolicitado, k -> new ArrayList<>()).add(msjExterno);
            System.out.println("[Ricart-Agrawala] " + idNodoLocal + " DIFIERE a " + msjExterno.getIdNodoOrigen()
                    + " (archivo " + archivoSolicitado + ")");
        } else {
            enviarPermiso(msjExterno.getIdNodoOrigen(), archivoSolicitado);
        }
    }

    private void evaluarPermisoRecibido(MensajeCoordinacion msjPermiso) {
        String archivo = msjPermiso.getNombreArchivo();
        Object candado = candadosDeEspera.get(archivo);
        if (candado == null) {
            return;
        }
        // Actualizar y notificar bajo el mismo monitor que usa el hilo en espera,
        // para evitar wakeups perdidos.
        synchronized (candado) {
            permisosRecibidos.merge(archivo, 1, Integer::sum);
            candado.notifyAll();
        }
    }

    // Pide acceso al resto de nodos VIVOS y bloquea hasta reunir el quórum.
    public void solicitarAccesoCritico(String archivo) {
        // Serializa las solicitudes locales del mismo archivo (invariante de R-A).
        ReentrantLock candadoLocal = candadoLocalPorArchivo.computeIfAbsent(archivo, k -> new ReentrantLock());
        candadoLocal.lock();

        int tiempoDeMiSolicitud = relojLogico.registrarEnvio();
        Set<String> peersDestino = membresia.obtenerPeersVivos();

        synchronized (this) {
            estadosPorArchivo.put(archivo, EstadoSeccionCritica.BUSCANDO);
            tiemposDeMisSolicitudes.put(archivo, tiempoDeMiSolicitud);
            permisosRecibidos.put(archivo, 0);
            peersContactados.put(archivo, new HashSet<>(peersDestino));
            candadosDeEspera.putIfAbsent(archivo, new Object());
        }

        for (String nodoDestino : peersDestino) {
            enviarMensajeA(nodoDestino, new MensajeCoordinacion(
                    MensajeCoordinacion.Tipo.SOLICITUD_ACCESO, tiempoDeMiSolicitud, idNodoLocal, archivo));
        }

        Object candado = candadosDeEspera.get(archivo);
        synchronized (candado) {
            // El quórum se recalcula contra los peers que SIGUEN vivos: si un nodo
            // cae mientras esperamos, deja de ser requerido y no hay deadlock.
            while (permisosRecibidos.getOrDefault(archivo, 0) < permisosRequeridos(archivo)) {
                try {
                    candado.wait(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        synchronized (this) {
            estadosPorArchivo.put(archivo, EstadoSeccionCritica.OCUPANDO);
        }
        System.out.println("[Lamport=" + relojLogico.obtenerTiempo() + "] ---> Nodo " + idNodoLocal
                + " ENTRA a sección crítica (" + archivo + ")");
    }

    // Cantidad de permisos aún requeridos: peers contactados que continúan vivos.
    private int permisosRequeridos(String archivo) {
        Set<String> contactados = peersContactados.getOrDefault(archivo, new HashSet<>());
        int requeridos = 0;
        for (String nodo : contactados) {
            if (membresia.estaVivo(nodo)) {
                requeridos++;
            }
        }
        return requeridos;
    }

    // Libera el recurso y envía permisos a los nodos que dejamos en espera
    public void liberarAccesoCritico(String archivo) {
        List<MensajeCoordinacion> solicitudesAprobadas;
        relojLogico.registrarEventoLocal();

        synchronized (this) {
            estadosPorArchivo.put(archivo, EstadoSeccionCritica.LIBERADO);
            tiemposDeMisSolicitudes.remove(archivo);
            solicitudesAprobadas = new ArrayList<>(solicitudesDiferidas.getOrDefault(archivo, new ArrayList<>()));
            solicitudesDiferidas.remove(archivo);
            permisosRecibidos.put(archivo, 0);
            peersContactados.remove(archivo);
        }

        System.out.println("[Lamport=" + relojLogico.obtenerTiempo() + "] <--- Nodo " + idNodoLocal
                + " SALE de sección crítica (" + archivo + ") | msgs coord [env=" + mensajesEnviados.get()
                + " recv=" + mensajesRecibidos.get() + "]");

        for (MensajeCoordinacion peticionPendiente : solicitudesAprobadas) {
            enviarPermiso(peticionPendiente.getIdNodoOrigen(), archivo);
        }

        ReentrantLock candadoLocal = candadoLocalPorArchivo.get(archivo);
        if (candadoLocal != null && candadoLocal.isHeldByCurrentThread()) {
            candadoLocal.unlock();
        }
    }

    private void enviarPermiso(String nodoDestino, String archivo) {
        int tiempoPermiso = relojLogico.registrarEnvio();
        enviarMensajeA(nodoDestino, new MensajeCoordinacion(
                MensajeCoordinacion.Tipo.PERMISO_CONCEDIDO, tiempoPermiso, idNodoLocal, archivo));
    }

    private void enviarMensajeA(String nodoDestino, MensajeCoordinacion mensaje) {
        boolean entregado = CanalCoordinacion.enviar(nodoDestino, mensaje);
        if (entregado) {
            mensajesEnviados.incrementAndGet();
        } else {
            System.err.println("[Coordinación] No se pudo entregar mensaje a " + nodoDestino
                    + " (posible caída/omisión). El quórum se reconfigura por membresía.");
        }
    }

    public long getMensajesEnviados() {
        return mensajesEnviados.get();
    }

    public long getMensajesRecibidos() {
        return mensajesRecibidos.get();
    }
}
