import os

# change dir to dir this script is in
os.chdir(os.path.dirname(__file__))

#make output folder if needed
if not os.path.exists('output'):
    os.makedirs('output')

#make input folder if needed
if not os.path.exists('input'):
    os.makedirs('input')

filename = 'aetherhub-export-helvault.csv'

if not os.path.exists('input/' + filename):
    print("Please place " + filename + " in the input folder.")
    exit()
else:
    input_file_path = os.path.join('input', filename)
    output_file_path = os.path.join('output', filename)


with open(input_file_path, 'r', encoding='utf-16') as f:
    content = f.read()

# Write the content to a new file using UTF-8 encoding
with open(output_file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("File re-encoded to UTF-8 successfully!")
