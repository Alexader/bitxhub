package parallel

func (exec *ParallelBlockExecutor) isDemandNumber(num uint64) bool {
	return exec.currentHeight+1 == num
}

func (exec *ParallelBlockExecutor) getDemandNumber() uint64 {
	return exec.currentHeight + 1
}
