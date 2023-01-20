package org.example;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;

public class Client {
    public static Logger logger = Logger.getLogger(Client.class.getName());
    private static int concurrency = 1;

    public static AtomicLong totalBytesTransferred = new AtomicLong(0);

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");

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

        LocalTime startTime = LocalTime.now();
        logger.info("Start time: " + startTime);

        sendFile(files);

        LocalTime endTime = LocalTime.now();
        logger.info("End time: " + endTime);

        calculateThroughput(startTime, endTime);
    }

    private static void sendFile(File[] files) {
        int numberOfFilesToSend = files.length;
        logger.info("Total number of files to send: " + numberOfFilesToSend);

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            try {
                Socket socket = new Socket("localhost", Server.PORT);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                Thread t = new ClientThread(files, dos, dis, concurrency);
                threads.add(t);
                t.start();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
        }

        for (Thread thread :
                threads) {
            try {
                thread.join();
            } catch (InterruptedException exception) {
                logger.log(Level.SEVERE, exception.getMessage());

            }
        }
    }

    private static void calculateThroughput(LocalTime startTime, LocalTime endTime) {
        long totalDuration = Duration.between(startTime, endTime).toSeconds();
        logger.info("Total Time taken(in seconds): " + totalDuration);
        double totalBytesTransferredInMB = totalBytesTransferred.doubleValue() / Math.pow(10, 6);
        logger.info("Total Bytes Transferred(in Mega Bytes[MB]): " +
                totalBytesTransferredInMB);
        double throughput = totalBytesTransferredInMB / totalDuration;
        logger.info("Throughput(MB per second): " + throughput);
    }
}

class ClientThread extends Thread {
    final private DataOutputStream dos;
    final private DataInputStream dis;
    final private File[] files;

    final private int concurrency;

    public ClientThread(File[] files, DataOutputStream dos, DataInputStream dis, int concurrency) {
        this.dis = dis;
        this.dos = dos;
        this.files = files;
        this.concurrency = concurrency;
    }

    @Override
    public void run() {
        try {
            LocalTime threadStartTime = LocalTime.now();
            Client.logger.info(ClientThread.currentThread().getName() + ": Started at " + threadStartTime);
            String[] split = ClientThread.currentThread().getName().split("-");
            int threadNumber = Integer.parseInt(split[1]);
            sendNumberOfFilesPerThread(threadNumber);
            int index = threadNumber;
            while (index < files.length) {
                long length = files[index].length();
                dos.writeLong(length);
                String fileName = files[index].getName();
                Client.logger.info(ClientThread.currentThread().getName() + ": Sending " + fileName + " file. " +
                        " Size: " + length);
                dos.writeUTF(fileName);

                FileInputStream fis = new FileInputStream(files[index]);
                CheckedInputStream checkedInputStream = new CheckedInputStream(fis, new CRC32C());
                sendFile(length, checkedInputStream);
                long checkSum = checkedInputStream.getChecksum().getValue();
                dos.writeLong(checkSum);
                Client.logger.info(ClientThread.currentThread().getName() + ": sent file " + fileName +
                        " of size " + files[index].length() + " checksum: " + checkSum);
                dos.flush();
                fis.close();
                checkedInputStream.close();
                int status = dis.readInt();
                if (status == 200) {
                    String serverReceiptTime = dis.readUTF();
                    Client.logger.info(ClientThread.currentThread().getName() +
                            ": File is received successfully at " + serverReceiptTime);
                    Client.totalBytesTransferred.addAndGet(files[index].length());
                }
                index += concurrency;
            }
            LocalTime threadFinishTime = LocalTime.now();
            Client.logger.info(ClientThread.currentThread().getName() + ": Finished at " + threadFinishTime +
                    ". Duration(in seconds): " + Duration.between(threadStartTime, threadFinishTime).toSeconds());
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

    private void sendNumberOfFilesPerThread(int threadNumber) throws IOException {
        int numberOfExtraFiles = files.length % concurrency;
        int numberOfFilesPerThread = files.length / concurrency;
        if (numberOfExtraFiles > 0) {
            if (threadNumber < numberOfExtraFiles) {
                numberOfFilesPerThread += 1;
            }
        }
        dos.writeLong(numberOfFilesPerThread);
        Client.logger.info(ClientThread.currentThread().getName() + ": Sending " + numberOfFilesPerThread + " files");
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