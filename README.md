with open('iris.data', 'r') as f_in, open('dataset.txt', 'w') as f_out:
    for line in f_in:
        parts = line.strip().split(',')
        if len(parts) == 5:
            features = ' '.join(parts[:4])  # أول 4 features، مفصولة بمسافات
            f_out.write(features + '\n')
print("Cleaned to dataset.txt – 150 lines ready!")
