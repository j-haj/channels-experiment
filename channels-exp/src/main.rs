use std::thread;

use rand::{thread_rng, Rng};
use rand::distributions::{LogNormal, Distribution};
use rayon::prelude::*;
use crossbeam::crossbeam_channel::{bounded, Sender, Receiver};

struct Coordinator {
    global_min: f64,
    update_receiver: Receiver<f64>,
    update_sender: Sender<f64>,
    broadcast_receiver: Receiver<f64>,
    broadcast_sender: Sender<f64>,
}

struct Worker {
    id: usize,
    local_min: f64,
    global_min: f64,
    update_receiver: Receiver<f64>,
    update_sender: Sender<f64>,
    dist: LogNormal,
}

struct Simulation {
    n_workers: usize,
}

impl Coordinator {
    // Create a Coordinator with the specified number of workers.
    pub fn new() -> Coordinator {
        let (update_sender, update_receiver) = bounded(0);
        let (broadcast_sender, broadcast_receiver) = bounded(0);

        Coordinator {
            global_min: std::f64::INFINITY,
            update_receiver: update_receiver,
            update_sender: update_sender,
            broadcast_receiver: broadcast_receiver,
            broadcast_sender: broadcast_sender,
        }
    }

    pub fn run(&mut self) {
        loop {
            let mut msgs: Vec<_> = self.update_receiver.iter().collect();
            msgs.iter().for_each(|v| {
                if *v < self.global_min {
                    self.global_min = *v;
                    self.broadcast_sender.send(self.global_min).unwrap();
                    println!("Coordinator updated min to {}",
                             self.global_min);
                }
            });
        }
    }

    pub fn broadcast_receiver(&self) -> Receiver<f64> {
        self.broadcast_receiver.clone()
    }

    pub fn update_sender(&self) -> Sender<f64> {
        self.update_sender.clone()
    }
}

impl Worker {

    pub fn new(id: usize,
               update_receiver: Receiver<f64>,
               update_sender: Sender<f64>) -> Worker {
        Worker { id: id,
                 local_min: std::f64::INFINITY,
                 global_min: std::f64::INFINITY,
                 update_receiver: update_receiver,
                 update_sender: update_sender,
                 dist: LogNormal::new(0., 1.),
        }
    }

    fn check_global(&mut self) {
        if !self.update_receiver.is_empty() {
            let msgs : Vec<_> = self.update_receiver.try_iter().collect();
            self.global_min = msgs.iter().fold(std::f64::INFINITY,
                                               |acc, x| acc.min(*x));
            println!("Received new min: {}", self.global_min);
        }
    }
        

    pub fn perform_update(&mut self) {
        // Check global
        self.check_global();

        // Get random value
        let v = self.dist.sample(&mut thread_rng());

        // Check with local best and recheck global
        if v < self.local_min {
            self.local_min = v;
            // Check global
            self.check_global();
            
            if self.local_min < self.global_min {
                self.update_sender.send(self.local_min).unwrap();
                println!("{} is sending {}", self.id, self.local_min);
                self.global_min = self.local_min;
                println!("{} updated min to {}", self.id, self.local_min);
            }
        }
    }
}

impl Simulation {
    pub fn new(n_workers: usize) -> Simulation {
        Simulation { n_workers: n_workers, }
    }

    pub fn run(&mut self) {
        // Run coordinator on separate thread
        let mut coordinator = Coordinator::new();

        let mut workers : Vec<_> = (0..self.n_workers)
            .into_iter()
            .map(|i| Worker::new(i,
                                 coordinator.broadcast_receiver(),
                                 coordinator.update_sender()))
            .collect();

        thread::spawn(move || {
            coordinator.run();
        });

        // Run worker updates
        let mut update_count = 0;
        loop {
            workers.par_iter_mut()
                .for_each(|w| w.perform_update());
            update_count += 1;
            if update_count % 10000 == 0 {
                println!("Update {}", update_count);
            }
        }
    }
}

fn main() {
    let mut sim = Simulation::new(5);
    println!("Running simulation...");
    sim.run();
}
