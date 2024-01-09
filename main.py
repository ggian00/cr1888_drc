import csv


class Table:
    def __init__(self):
        self.data_dict = {}

    def __len__(self):
        """Returns the length of the table."""
        keys = list(self.data_dict.keys())
        if len(keys) == 0:
            return 0
        return len(self.data_dict[keys[0]])

    def remove(self, idx):
        """Remove row."""
        for key in self.data_dict:
            del self.data_dict[key][idx]

    def read_csv(self, file: str):
        with open(file) as f:
            reader = csv.DictReader(f)
            self.data_dict = {fieldname: [] for fieldname in reader.fieldnames}
            for row in reader:
                for column in self.data_dict:
                    self.data_dict[column].append(row[column])

    def write_csv(self, file: str):
        with open(file, "w") as f:
            writer = csv.DictWriter(
                f, quoting=csv.QUOTE_ALL, fieldnames=self.data_dict.keys()
            )
            writer.writeheader()
            for i in range(len(self)):
                row = {key: self.data_dict[key][i] for key in self.data_dict}
                writer.writerow(row)

    def right_join_csv(self, file: str, on: str = None) -> "Table":
        if len(self) == 0:
            raise RuntimeError("Cannot join with empty table.")

        if on is None:
            on = next(iter(self.data_dict))  # get first key by default
        elif on not in self.data_dict:
            raise KeyError(f'Left table does not have "{on}" as a column.')

        with open(file) as f:
            reader = csv.DictReader(f)
            right_data_dict = {fieldname: [] for fieldname in reader.fieldnames}

            if on not in right_data_dict:
                raise KeyError(f'Right join csv does not have "{on}" as a column.')

            for row in reader:
                if row[on] not in self.data_dict[on]:
                    continue
                for column in right_data_dict:
                    right_data_dict[column].append(row[column])

        new_table = Table()
        new_table.data_dict = right_data_dict
        return new_table

    def __repr__(self):
        return str(self.data_dict)


if __name__ == "__main__":
    print("Verbose Table contents. Result is writter to file <filename>_new.csv")
    sample = Table()
    sample.read_csv("sample.csv")
    print(f"{sample=}")

    customers = sample.right_join_csv("customer.csv")
    print(f"{customers=}")
    customers.write_csv("customer_new.csv")

    invoices = customers.right_join_csv("invoice.csv")
    print(f"{invoices=}")
    invoices.write_csv("invoice_new.csv")

    invoice_items = invoices.right_join_csv("invoice_item.csv", on="INVOICE_CODE")
    print(f"{invoice_items=}")
    invoice_items.write_csv("invoice_item_new.csv")

    print("\nTest for one customer. (No writing to file)")
    sample.remove(0)
    print(f"{sample}")
    customers = sample.right_join_csv("customer.csv")
    invoices = customers.right_join_csv("invoice.csv")
    invoice_items = invoices.right_join_csv("invoice_item.csv", on="INVOICE_CODE")
    print(f"{customers=}")
    print(f"{invoices=}")
    print(f"{invoice_items=}")
