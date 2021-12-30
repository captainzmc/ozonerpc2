package org.apache.hadoop.ozone.examples;

import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class LaunchSync extends Thread {
    private final ServerSocket serverSocket;
    private final CountDownLatch latch;
    private final int port;
    private final List<String> group;

    public LaunchSync(int port, List<String> group) throws IOException {
        this.port = port;
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(1000*60*5);
        this.group = group;
        latch = new CountDownLatch(group.size());
    }

    public void run() {
        while (true) {
            try {
                System.out.println("LaunchSync Listening Port: " + serverSocket.getLocalPort() + "...");
                Socket server = serverSocket.accept();
                System.out.println("Receive signal: " + server.getRemoteSocketAddress());
                DataInputStream in = new DataInputStream(server.getInputStream());
                System.out.println(in.readUTF());
                latch.countDown();
                DataOutputStream out = new DataOutputStream(server.getOutputStream());
                out.writeUTF("ACK: " + server.getLocalSocketAddress());
                server.close();
                if (latch.getCount() == 0) {
                    return;
                }
            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public void sendReady() {
        for (String hostName : group) {
            try
            {
                System.out.println("connect: " + hostName+ ":" + port);
                Socket client = new Socket(hostName, port);
                OutputStream outToServer = client.getOutputStream();
                DataOutputStream out = new DataOutputStream(outToServer);
                out.writeUTF("READY: " + client.getLocalSocketAddress());
                InputStream inFromServer = client.getInputStream();
                DataInputStream in = new DataInputStream(inFromServer);
                System.out.println("GET REPLY:  " + in.readUTF());
                client.close();
            }catch(IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    public void waitAllReady() throws InterruptedException {
        latch.await();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ArrayList<String> group = new ArrayList<>();
        group.add("127.0.0.1");
        LaunchSync launchSync = new LaunchSync(55666, group);
        launchSync.start();
        launchSync.sendReady();
        launchSync.waitAllReady();
        System.out.println("DONE");
    }
}
