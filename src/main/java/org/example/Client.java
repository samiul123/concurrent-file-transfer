package org.example;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;

public class Client {
    public static Logger logger = Logger.getLogger(Client.class.getName());
    private static int concurrency = 1;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0) {
            logger.log(Level.SEVERE, "Source directory location is required");
            return;
        } else if (args.length > 2) {
            logger.log(Level.SEVERE, "Unnecessary arguments given");
            return;
        }

        String srcFolder = args[0];
        Path srcPath = Path.of(srcFolder);
        if (!Files.isDirectory(srcPath) || Files.notExists(srcPath)) {
            logger.log(Level.SEVERE, "Directory: " + srcFolder + " does not exists");
            return;
        }

        Optional<File[]> optionalFiles = Optional.ofNullable(new File(srcFolder).listFiles());
        if (optionalFiles.isEmpty()) {
            logger.log(Level.SEVERE, "Directory: " + srcFolder + " does not have any file");
            return;
        }
        File[] files = optionalFiles.get();

        if (args.length == 2) {
            concurrency = Integer.parseInt(args[1]);
        }
        concurrency = Math.min(concurrency, files.length);

        logger.info("Concurrency: " + concurrency);
        sendFile(files);
    }

    private static void sendFile(File[] files) {
        int numberOfFilesToSend = files.length;
        logger.info("Total number of files to send: " + numberOfFilesToSend);
        int numberOfSockets = (int) Math.ceil((double) numberOfFilesToSend / concurrency);
        logger.info("Total Number of connections: " + numberOfSockets);
        int from = 0;
        for (int i = 0; i < numberOfSockets; i++) {
            try {
                Socket socket = new Socket("localhost", Server.PORT);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                File[] copy = Arrays.copyOfRange(files, from, Math.min(from + concurrency, files.length));
                from += concurrency;
                Thread t = new ClientThread(copy, dos, dis);
                t.start();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
        }
    }
}

class ClientThread extends Thread {
    final private DataOutputStream dos;
    final private DataInputStream dis;
    final private File[] files;

    public ClientThread(File[] files, DataOutputStream dos, DataInputStream dis) {
        this.dis = dis;
        this.dos = dos;
        this.files = files;
    }

    @Override
    public void run() {
        try {
            LocalTime threadStartTime = LocalTime.now();
            Client.logger.info(ClientThread.currentThread().getName() + ": Started at " + threadStartTime);
            dos.writeLong(files.length);
            Client.logger.info(ClientThread.currentThread().getName() + ": Sending " + files.length + " files");
            for (File file :
                    files) {
                long length = file.length();
                dos.writeLong(length);
                String fileName = file.getName();
                Client.logger.info(ClientThread.currentThread().getName() + ": Sending " + fileName + " file. " +
                        " Size: " + length);
                dos.writeUTF(fileName);

                FileInputStream fis = new FileInputStream(file);
                CheckedInputStream checkedInputStream = new CheckedInputStream(fis, new CRC32C());
                sendFile(length, checkedInputStream);
                long checkSum = checkedInputStream.getChecksum().getValue();
                dos.writeLong(checkSum);
                Client.logger.info(ClientThread.currentThread().getName() + ": sent file " + fileName +
                        " of size " + file.length() + " checksum: " + checkSum);
                dos.flush();
                fis.close();
                checkedInputStream.close();
                int status = dis.readInt();
                if (status == 200) {
                    Client.logger.info(ClientThread.currentThread().getName() + ": File is received successfully");
                }
            }
            LocalTime threadFinishTime = LocalTime.now();
            Client.logger.info(ClientThread.currentThread().getName() + ": Finished at " + threadFinishTime +
                    ". Duration: " + Duration.between(threadStartTime, threadFinishTime).toMillis());
        } catch (IOException e) {
            Client.logger.log(Level.SEVERE, e.getMessage());
        } finally {
            try {
                if (dis != null) dis.close();
                if (dos != null) dos.close();
            } catch (IOException e) {
                Client.logger.log(Level.SEVERE, e.getMessage());
            }
        }

    }

    private void sendFile(long length, CheckedInputStream checkedInputStream) throws IOException {
        byte[] buff = new byte[4096];
        int len = 0;
        while (length > 0 && (len = checkedInputStream.read(buff, 0,
                (int) Math.min(buff.length, length))) != -1) {
            dos.write(buff, 0, len);
            length -= len;
        }
    }
}