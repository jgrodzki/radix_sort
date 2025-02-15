# Parallel LSD radix sort

This project contains a parallel implementation of LSD radix sort algorithm written in Rust.

## Prerequisites

In order to run the project you need to install Rust along with cargo package manager.

If you are using Windows, you can do so by installing the rustup toolchain from the [official website](https://www.rust-lang.org/). The script will guide you through the installation.

If you are using Linux, you can install either Rust or rustup toolchain from your distribution's package manager.

## How to run

This project contains a small demo of sorting performance on various data set sizes.

You can run it in relase mode using the following command:

`cargo run -r`

## How to test

This project contains a suite of tests that aim to check the validity of algorithm's output for all of the types with built-in support. You can run it using the following command: 

`cargo test` 
