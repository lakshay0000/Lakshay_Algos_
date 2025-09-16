import random

def first_three_all_even(seq):
    evens = {2, 4, 6}
    first_three = seq[:3]
    return all(num in evens for num in first_three)

def simulate(n_trials=100_000):
    success_count = 0

    for _ in range(n_trials):
        # Generate a random permutation of 1 to 6 (all unique)
        sequence = random.sample(range(1, 7), 6)

        # Check if first three numbers are all even
        if first_three_all_even(sequence):
            success_count += 1

    probability = success_count / n_trials
    return probability

# Run simulation
prob = simulate()
print(f"Estimated Probability: {prob:.5f}")
