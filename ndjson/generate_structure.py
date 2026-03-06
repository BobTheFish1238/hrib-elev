import os

base_path = r"C:\Users\bobth\Documents\Hrib-Habitat\ndjson2\Morels"
modified_date = "2026-02-23"

print('\t\t\t"Morels": {')

for folder in sorted(os.listdir(base_path)):
    folder_path = os.path.join(base_path, folder)

    if os.path.isdir(folder_path):
        print(f'\t\t\t\t"{folder}": [')

        files = sorted(f for f in os.listdir(folder_path) if f.endswith(".ndjson"))

        for i, file in enumerate(files):
            comma = "," if i < len(files) - 1 else ""
            print(f'\t\t\t\t\t{{name: "{file}", modified: "{modified_date}"}}{comma}')

        print('\t\t\t\t],')

print('\t\t\t}')