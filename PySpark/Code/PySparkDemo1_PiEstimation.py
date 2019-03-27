import random

# Filter will take a input and output True and False (randomly)
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

NUM_SAMPLES = 100
count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()


print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)


# ModuleNotFoundError: No module named 'resource'
# Recommendation is to downgrade spark to 2.3.2 (Currently it is 2.4.0)