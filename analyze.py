#!/usr/bin/env python3

from evalys.jobset import JobSet
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import evalys

js = JobSet.from_csv("./out/jobs.csv")
js.plot(with_details=True)
plt.savefig("./jobs.png")
