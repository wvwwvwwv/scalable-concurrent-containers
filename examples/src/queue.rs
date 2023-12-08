#[cfg(test)]
mod examples {
    use scc::Queue;

    #[test]
    fn single_threaded() {
        let workload_size = 256;
        let queue: Queue<isize> = Queue::default();
        for i in 1..workload_size {
            queue.push(i);
        }
        let mut expected = 1;
        while let Some(popped) = queue.pop() {
            assert_eq!(**popped, expected);
            expected = **popped + 1;
        }
        assert_eq!(expected, workload_size);
        assert!(queue.is_empty());
    }
}
