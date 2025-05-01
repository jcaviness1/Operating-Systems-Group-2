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

