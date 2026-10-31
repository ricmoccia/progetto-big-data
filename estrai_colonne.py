import csv

input_path = "/home/riccardo/progettoBigData/dataset/cars_100k.csv"
output_path = "/home/riccardo/progettoBigData/dataset/cars_100k_clean.csv"

with open(input_path, newline='', encoding='utf-8') as infile, open(output_path, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=["make_name", "model_name", "price", "year"])
    writer.writeheader()
    for row in reader:
        try:
            writer.writerow({
                "make_name": row["make_name"],
                "model_name": row["model_name"],
                "price": row["price"],
                "year": row["year"]
            })
        except KeyError:
            pass  # salta righe malformate

