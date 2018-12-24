use std::thread;
use std::time::Duration;

use rand::{thread_rng, Rng};
use rand::distributions::{LogNormal, Distribution};
use rayon::prelude::*;
use crossbeam::crossbeam_channel::{bounded, Sender, Receiver};

struct Channel<T> where T: Send + Copy + Clone {
    send: Sender<T>,
    receive: Receiver<T>,
}

struct Coordinator {
    global_min: f64,
    broadcast_channels: Vec<Sender<f64>>,
    update_receiver: Receiver<f64>,
    update_sender: Sender<f64>,
    n_workers: usize,
}

struct Worker {
    id: usize,
    local_min: f64,
    global_min: f64,
    channel: Channel<f64>,
    dist: LogNormal,
}

struct Simulation {
    n_workers: usize,
}

impl<T> Channel<T> where T: Send + Copy + Clone {
    pub fn new(sender: Sender<T>, receiver: Receiver<T>) -> Channel<T> {
        Channel { send: sender, receive: receiver }
    }
    pub fn sender(&self) -> &Sender<T> { &self.send }
    pub fn receiver(&self) -> &Receiver<T> { &self.receive }
}

impl Coordinator {
    // Create a Coordinator with the specified number of workers.
    pub fn new(n_workers: usize) -> Coordinator {
        let (update_sender, update_receiver) = bounded(n_workers);

        Coordinator {
            global_min: std::f64::INFINITY,
            broadcast_channels: Vec::new(),
            update_sender: update_sender,
            update_receiver: update_receiver,
            n_workers: 0,
        }
    }

    pub fn run2(&mut self) {
        loop {
            select! {
                recv(self.update_receiver) -> msg => {

                }
                send(self.
            }
        }
    
    pub fn run(&mut self) {
        loop {
            if let Ok(mut val) = self.update_receiver.recv() {
                if self.update_receiver.len() > 0 {
                    let mut msgs: Vec<_> = self.update_receiver.try_iter().collect();
                    println!("Coordinator received {} messages. {} messages \
                              in the channel currently.",
                             msgs.len(),
                             self.update_receiver.len());
                    msgs.iter().for_each(|v| {
                        if *v < val {
                            val = *v;
                        }
                    });
                }
                self.global_min = val;
                println!("Coordinator attempting to broadcast");
                self.broadcast_channels
                    .par_iter()
                    .for_each(|c| c.send_timeout(self.global_min,
                                                 Duration::from_millis(5))
                              .unwrap());
                println!("Coordinator updated min to {}", val);
            }
        }
    }

    pub fn new_worker(&mut self) -> Worker {
        let id = self.n_workers;
        self.n_workers += 1;
        let (b_send, b_recv) = bounded(1);
        let channel = Channel::new(self.update_sender(), b_recv);
        self.broadcast_channels.push(b_send);
        Worker::new(id, channel)
    }

    ///! Creates a clone of the update sender channel. All workers should
    ///! use this channel to send updates to the coordinator.
    pub fn update_sender(&self) -> Sender<f64> {
        self.update_sender.clone()
    }
}

impl Worker {

    pub fn new(id: usize,
               channel: Channel<f64>) -> Worker {
        Worker { id: id,
                 local_min: std::f64::INFINITY,
                 global_min: std::f64::INFINITY,
                 channel: channel,
                 dist: LogNormal::new(0., 1.),
        }
    }

    fn check_global(&mut self) {
        if !self.channel.receiver().is_empty() {
            let global_min = self.channel.receiver().recv().unwrap();

            if global_min > self.local_min {
                self.channel.sender().send_timeout(self.local_min,
                                                   Duration::from_millis(10))
                    .unwrap();
                println!("{} sent update of {} for global min",
                         self.id,
                         self.local_min);
            } else {
                println!("{} received new min: {}", self.id, self.local_min);
                self.local_min = global_min;
            }
        } else {
            println!("{} sending min to coordinator", self.id);
            self.channel.sender().send_timeout(self.local_min,
                                               Duration::from_millis(10))
                .unwrap();
            println!("{} min sent", self.id);
        }
    }
        

    pub fn perform_update(&mut self) {
        // Get random value
        let v = self.dist.sample(&mut thread_rng());
        self.local_min = self.local_min.min(v);
        let send_res = self.channel.sender().try_send(self.local_min);
        match send_res {
            Ok(_) => self.global_min = self.channel.receiver().recv().unwrap(),

        self.global_min = self.channel.receiver().recv().unwrap();
    }

    pub fn print_status(&self) {
        println!("{} min is {}", self.id, self.local_min);
    }
}

impl Simulation {
    pub fn new(n_workers: usize) -> Simulation {
        Simulation { n_workers: n_workers, }
    }

    pub fn run(&mut self) {
        // Run coordinator on separate thread
        let mut coordinator = Coordinator::new(self.n_workers);

        let mut workers : Vec<_> = (0..self.n_workers)
            .into_iter()
            .map(|i| coordinator.new_worker())
            .collect();

        thread::spawn(move || {
            println!("Starting coordinator.");
            coordinator.run();
            println!("Coordinator exiting...");
        });

        // Run worker updates
        let mut update_count = 0;
        loop {
            workers.par_iter_mut()
                .for_each(|w| w.perform_update());
            update_count += 1;
            if update_count % 10000 == 0 {
                println!("Update {}", update_count);
                workers.iter().for_each(|w| w.print_status());
            }
        }
    }
}

fn main() {
    let mut sim = Simulation::new(5);
    println!("Running simulation...");
    sim.run();
}
