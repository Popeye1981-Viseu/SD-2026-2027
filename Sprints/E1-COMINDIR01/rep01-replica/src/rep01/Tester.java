package rep01;

import java.net.*;
import java.nio.charset.StandardCharsets;

public class Tester {
    public static void main(String[] args) throws Exception {
        MulticastSocket s = new MulticastSocket();
        InetAddress group = InetAddress.getByName("230.0.0.1");
        s.setTimeToLive(1);

        // Recebe os registos a enviar como argumentos; ex: java rep01.Tester 9 11 10
        for (String seqStr : args) {
            long seq = Long.parseLong(seqStr);
            String msg = seq + ";S99;20.0;2026-10-01T10:00:" + String.format("%02d", seq);
            byte[] b = msg.getBytes(StandardCharsets.UTF_8);
            s.send(new DatagramPacket(b, b.length, group, 6789));
            System.out.println("Enviado: " + msg);
            Thread.sleep(500);   // pausa para veres a reação passo a passo
        }
        s.close();
    }
}
