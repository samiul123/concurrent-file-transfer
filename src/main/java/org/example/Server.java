package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalTime;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;

@SuppressWarnings("InfiniteLoopStatement")
public class Server {
    public static final int PORT = 8000;
    public static Logger logger = Logger.getLogger(Server.class.getName());

    public static void main(String[] args) throws IOException {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                logger.info("Server is listening on port: " + PORT);
                Socket socket = serverSocket.accept();
                logger.info("Connection established: " + socket.getRemoteSocketAddress());
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                Thread t = new ClientHandler(dis, dataOutputStream);
                t.start();
            }
        } catch (Exception exception) {
            logger.log(Level.SEVERE, exception.getMessage());
        }
    }
}

class ClientHandler extends Thread {
    final String dir = "server/";
    final DataInputStream dis;
    final DataOutputStream dos;

    public ClientHandler(DataInputStream dis, DataOutputStream dos) {
        this.dis = dis;
        this.dos = dos;
    }

    @Override
    public void run() {
        Server.logger.info(ClientHandler.currentThread().getName() + " started");
        try {
            long totalLength = dis.readLong();
            Server.logger.info(ClientHandler.currentThread().getName() + " number of files to parse: "
                    + totalLength);
            for (int i = 0; i < totalLength; i++) {
                long fileLength = dis.readLong();
                String fileName = dis.readUTF();
                Server.logger.info(ClientHandler.currentThread().getName() + " reading File: " + fileName +
                        " size: " + fileLength);
                File file = new File(dir + fileName);
                FileOutputStream fos = new FileOutputStream(file);
                CheckedInputStream checkedInputStream = new CheckedInputStream(dis, new CRC32C());
                saveFile(fileLength, fos, checkedInputStream);
                long receivedChecksum = dis.readLong();
                Server.logger.info(ClientHandler.currentThread().getName() + " File Received. Name: " + fileName +
                        " Size: " + file.length() + " Checksum: " +
                        (checkedInputStream.getChecksum().getValue() == receivedChecksum ? "matched" : "not matched"));
                fos.close();

                if (checkedInputStream.getChecksum().getValue() == receivedChecksum) {
                    dos.writeInt(200);
                    dos.writeUTF(String.valueOf(LocalTime.now()));
                    dos.flush();
                }
            }
        } catch (Exception e) {
            Server.logger.log(Level.SEVERE, "Server: " + e);
        } finally {
            try {
                if (dis != null) dis.close();
                if (dos != null) dos.close();
            } catch (IOException exception) {
                Server.logger.log(Level.SEVERE, exception.getMessage());
            }
        }
    }

    private static void saveFile(long fileLength,
                                 FileOutputStream fos,
                                 CheckedInputStream checkedInputStream) throws IOException {
        byte[] buff = new byte[4096];
        int len = 0;
        while ((len = checkedInputStream.read(buff, 0, (int) Math.min(fileLength, buff.length))) > 0
                && fileLength > 0) {
            fos.write(buff, 0, len);
            fos.flush();
            fileLength -= len;
        }
    }
}
