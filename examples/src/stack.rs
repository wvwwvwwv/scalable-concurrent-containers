#[cfg(test)]
mod examples {
    use scc::Stack;

    #[test]
    fn single_threaded() {
        let workload_size = 256;
        let stack: Stack<isize> = Stack::default();
        for i in 1..workload_size {
            stack.push(i);
        }
        let mut expected = workload_size - 1;
        while let Some(popped) = stack.pop() {
            assert_eq!(**popped, expected);
            expected = **popped - 1;
        }
        assert_eq!(expected, 0);
        assert!(stack.is_empty());
    }
}
