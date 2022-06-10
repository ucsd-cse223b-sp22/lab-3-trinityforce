import matplotlib.pyplot as plt
import numpy as np

def log_lock_6000():
    op_count = np.array([1500, 3000, 4500, 6000])
    lock_rw7 = np.array([14.930, 29.791, 44.279, 58.267])
    log_rw7 = np.array([18.637, 39.349, 59.221, 85.278])
    lock_rw3 = np.array([14.506, 28.781, 42.347, 56.472])
    log_rw3 = np.array([16.254, 35.588, 55.881, 78.659])
    fig = plt.figure()
    plt.xlabel("Number of operations")
    plt.ylabel("Time")
    plt.title("#Opration v.s. Time")
    plt.plot(op_count, lock_rw3, 'r--', linewidth=2, label = "lock-7:3")
    plt.plot(op_count, log_rw3, 'r-', linewidth=2, label = "log-7:3")
    plt.plot(op_count, lock_rw7, 'b--', linewidth=2, label = "lock-3:7")
    plt.plot(op_count, log_rw7, 'b-', linewidth=2, label = "log-3:7")
    plt.plot(op_count, log_rw3-lock_rw3, 'r:', linewidth=2, label = "diff-log-lock-7:3")
    plt.plot(op_count, log_rw7-lock_rw7, 'b:', linewidth=2, label = "diff-log-lock-3:7")
    plt.legend()
    fig.savefig("log_lock_performance_6000.png")
    plt.close(fig)

def log_lock_10000():
    op_count = np.array([1500, 3000, 4500, 6000, 10000])
    lock_rw7 = np.array([14.930, 29.791, 44.279, 58.267, 142.495])
    log_rw7 = np.array([18.637, 39.349, 59.221, 85.278, 204.917])
    lock_rw3 = np.array([14.506, 28.781, 42.347, 56.472, 134.691])
    log_rw3 = np.array([16.254, 35.588, 55.881, 78.659, 197.379])
    fig = plt.figure()
    plt.xlabel("Number of operations")
    plt.ylabel("Time")
    plt.title("#Opration v.s. Time")
    plt.plot(op_count, lock_rw3, 'r--', linewidth=2, label = "lock-7:3")
    plt.plot(op_count, log_rw3, 'r-', linewidth=2, label = "log-7:3")
    plt.plot(op_count, lock_rw7, 'b--', linewidth=2, label = "lock-3:7")
    plt.plot(op_count, log_rw7, 'b-', linewidth=2, label = "log-3:7")
    plt.plot(op_count, log_rw3-lock_rw3, 'r:', linewidth=2, label = "diff-log-lock-7:3")
    plt.plot(op_count, log_rw7-lock_rw7, 'b:', linewidth=2, label = "diff-log-lock-3:7")
    plt.legend()
    fig.savefig("log_lock_performance_10000.png")
    plt.close(fig)


def log_lock_concurrency():
    num_client = np.array([1, 3, 5, 8, 10])
    lock_rw37 = np.array([29.508, 32.214, 44.836, 70.437, 87.714])
    log_rw37 = np.array([37.939, 52.426, 82.995, 172.922, 241.590])
    lock_rw73 = np.array([28.677, 31.666, 42.527, 67.055, 83.873])
    log_rw73 = np.array([34.107, 47.653, 78.562, 162.807, 234.406])
    fig = plt.figure()
    plt.xlabel("Number of clients")
    plt.ylabel("Time")
    plt.title("#Client v.s. Time")
    plt.plot(num_client, lock_rw73, 'r--', linewidth=2, label = "lock-7:3")
    plt.plot(num_client, log_rw73, 'r-', linewidth=2, label = "log-7:3")
    plt.plot(num_client, lock_rw37, 'b--', linewidth=2, label = "lock-3:7")
    plt.plot(num_client, log_rw37, 'b-', linewidth=2, label = "log-3:7")
    plt.plot(num_client, log_rw37-lock_rw37, 'r:', linewidth=2, label = "diff-log-lock-7:3")
    plt.plot(num_client, log_rw73-lock_rw73, 'b:', linewidth=2, label = "diff-log-lock-3:7")
    plt.legend()
    fig.savefig("log_lock_concurrency.png")
    plt.close(fig)


def log_lock_speedup():
    num_client = np.array([1, 3, 5, 8, 10])
    lock_rw37 = np.array([1.000, 2.748, 3.291, 3.351, 3.364])
    log_rw37 = np.array([1.000, 2.171, 2.286, 1.755, 1.570])
    lock_rw73 = np.array([1.000, 2.796, 3.469, 3.520, 3.518])
    log_rw73 = np.array([1.000, 2.147, 2.171, 1.676, 1.455])
    fig = plt.figure()
    
    plt.xlabel("Number of clients")
    plt.ylabel("Scaling ratio")
    plt.title("#Client v.s. Scaling ratio")
    plt.plot(num_client, lock_rw73, 'r--', linewidth=2, label = "lock-7:3")
    plt.plot(num_client, log_rw73, 'r-', linewidth=2, label = "log-7:3")
    plt.plot(num_client, lock_rw37, 'b--', linewidth=2, label = "lock-3:7")
    plt.plot(num_client, log_rw37, 'b-', linewidth=2, label = "log-3:7")
    plt.plot(num_client, num_client, 'k:', linewidth=2, label = "ideal")
    plt.legend()
    fig.savefig("log_lock_speedup.png")
    plt.close(fig)


log_lock_6000()
log_lock_10000()
log_lock_concurrency()
log_lock_speedup()

1	&	29.508	(0.775)	&   37.939	(0.157) \\
3	&	32.214	(0.587)	&   52.426	(0.321) \\
5	&	44.836	(0.386)	&   82.995	(0.743) \\
8	&	70.437	(0.602)	&   172.922	(1.778) \\
10	&	87.714	(1.065)	&   241.590	(1.558) \\


1	& 28.677 (0.405) &   34.107  (0.242) \\
3	& 31.666 (0.127) &   47.653  (0.183) \\
5	& 42.527 (0.161) &   78.562  (1.229) \\
8	& 67.055 (0.794) &   162.807 (1.156) \\
10	& 83.873 (0.079) &   234.406 (1.556) \\

1 & 1.000 & 1.000 & 1.000 & 1.000 \\
3 & 2.748 & 2.171 & 2.796 & 2.147 \\
5 & 3.291 & 2.286 & 3.469 & 2.171 \\
8 & 3.351 & 1.755 & 3.520 & 1.676 \\
10 & 3.364 & 1.570 & 3.518 & 1.455 \\