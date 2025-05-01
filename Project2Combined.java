import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

// ==== Part 1: Simulate Process Threads ====

class ProcessThread extends Thread {
    int pid, burstTime;

    // Constructor to initialize process ID and burst time
    public ProcessThread(int pid, int burstTime) {
        this.pid = pid;
        this.burstTime = burstTime;
    }

    @Override
    public void run() {
        // Simulate process execution
        System.out.println("Process " + pid + " started.");
        try {
            Thread.sleep(burstTime * 1000); // Simulate CPU burst time
        } catch (InterruptedException e) {}
        System.out.println("Process " + pid + " finished.");
    }
}

// ==== Part 2: Producer-Consumer with Clean Logging ====

class BoundedBuffer {
    private final Queue<Integer> buffer = new LinkedList<>();
    private final Semaphore emptySlots; // Semaphore for empty slots in the buffer
    private final Semaphore fullSlots;  // Semaphore for full slots in the buffer
    private final ReentrantLock mutex = new ReentrantLock(); // Lock to protect buffer
    private int itemCounter = 1; // Counter to track produced items

    // Constructor to initialize semaphores for the buffer
    public BoundedBuffer(int capacity) {
        this.emptySlots = new Semaphore(capacity); // Capacity of the buffer
        this.fullSlots = new Semaphore(0); // Initially no items in the buffer
    }

    // Method for the producer to add an item to the buffer
    public void produce() throws InterruptedException {
        System.out.println("[Producer] Waiting to add item " + itemCounter);
        emptySlots.acquire(); // Wait for an empty slot
        mutex.lock(); // Acquire lock before modifying the buffer
        try {
            int item = itemCounter++;
            buffer.add(item); // Add item to the buffer
            System.out.println("[Producer] Produced item: " + item);
        } finally {
            mutex.unlock(); // Release lock
            fullSlots.release(); // Signal a full slot
        }
    }

    // Method for the consumer to remove an item from the buffer
    public void consume() throws InterruptedException {
        System.out.println("[Consumer] Waiting to consume item");
        fullSlots.acquire(); // Wait for a full slot
        mutex.lock(); // Acquire lock before modifying the buffer
        try {
            int item = buffer.remove(); // Remove item from the buffer
            System.out.println("[Consumer] Consumed item: " + item);
        } finally {
            mutex.unlock(); // Release lock
            emptySlots.release(); // Signal an empty slot
        }
    }
}

// ==== Part 3: Producer Thread ====

class Producer extends Thread {
    private final BoundedBuffer buffer;
    private final int produceCount; // Number of items to produce

    public Producer(BoundedBuffer buffer, int produceCount) {
        this.buffer = buffer;
        this.produceCount = produceCount;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < produceCount; i++) {
                buffer.produce(); // Produce an item
                Thread.sleep((int)(Math.random() * 1000)); // Random delay
            }
            System.out.println("[Producer] Done producing.");
        } catch (InterruptedException e) {
            System.out.println("[Producer] Interrupted.");
        }
    }
}

// ==== Part 4: Consumer Thread ====

class Consumer extends Thread {
    private final BoundedBuffer buffer;
    private final int consumeCount; // Number of items to consume

    public Consumer(BoundedBuffer buffer, int consumeCount) {
        this.buffer = buffer;
        this.consumeCount = consumeCount;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < consumeCount; i++) {
                buffer.consume(); // Consume an item
                Thread.sleep((int)(Math.random() * 1500)); // Random delay
            }
            System.out.println("[Consumer] Done consuming.");
        } catch (InterruptedException e) {
            System.out.println("[Consumer] Interrupted.");
        }
    }
}
//==== Part 5: Main Method ====

public class Project2Combined {
    public static void main(String[] args) {
        // === Part 5.1: Simulate Process Threads ===
        System.out.println("Starting process threads...");
        ProcessThread[] processes = {
            new ProcessThread(1, 2), // Process 1 with 2-second burst time
            new ProcessThread(2, 3), // Process 2 with 3-second burst time
            new ProcessThread(3, 1)  // Process 3 with 1-second burst time
        };
        for (ProcessThread p : processes) {
            p.start(); // Start each process
        }
        for (ProcessThread p : processes) {
            try {
                p.join(); // Wait for process to finish
            } catch (InterruptedException e) {}
        }
        System.out.println("Process threads completed.\n");

        // === Part 5.2: Producer-Consumer Simulation ===
        System.out.println("Starting Producer - Consumer simulation...");

        final int bufferCapacity = 3; // Max buffer size
        final int itemsToProduce = 5; // Items to produce and consume

        BoundedBuffer buffer = new BoundedBuffer(bufferCapacity);

        Producer producer = new Producer(buffer, itemsToProduce); // Create producer
        Consumer consumer = new Consumer(buffer, itemsToProduce); // Create consumer

        producer.start(); // Start producer thread
        consumer.start(); // Start consumer thread

        try {
            producer.join(); // Wait for producer to finish
            consumer.join(); // Wait for consumer to finish
        } catch (InterruptedException e) {
            System.out.println("Main thread interrupted.");
        }

        System.out.println("Producer - Consumer simulation completed.");
    }
}