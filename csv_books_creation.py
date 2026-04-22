import csv
import random
from faker import Faker
from datetime import datetime

fake = Faker("it_IT")

categories = [
    "Narrativa", "Scienza", "Tecnologia", "Storia",
    "Fantasy", "Educazione", "Thriller"
]

file_name = f"/Volumes/workspace/default/bookstore_dataset/books-csv-new/books{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

def generate_csv(filename=file_name, num_rows=5):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=';')

        # Header
        writer.writerow(["book_id", "title", "author", "category", "price"])

        for i in range(1, num_rows + 1):
            title = fake.sentence(nb_words=4).rstrip(".")
            author = fake.name()
            category = random.choice(categories)
            price = round(random.uniform(5.0, 50.0), 2)

            writer.writerow([f"B{i}", title, author, category, price])

    print(f"File '{filename}' generato con {num_rows} righe.")

if __name__ == "__main__":
    generate_csv(num_rows=5)