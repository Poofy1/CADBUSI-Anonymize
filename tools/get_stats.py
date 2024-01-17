import pandas as pd
import os
env = os.path.dirname(os.path.abspath(__file__))

# Load data
file_path = f'{env}/input_file/filtered_data.csv'  # Please make sure the file path is correct
data = pd.read_csv(file_path)


# Calculate the statistics for unique patients
unique_patients = data['PatientID'].nunique()
unique_accessions = data['Accession'].nunique()

# Calculate average accessions per patient
average_accessions_per_patient = unique_accessions / unique_patients if unique_patients > 0 else 0

# Patients with Malignant, Benign, or None pathology
malignant = data[data['Pathology'] == 'Malignant']['PatientID'].nunique()
benign = data[data['Pathology'] == 'Benign']['PatientID'].nunique()
none = data[data['Pathology'].isnull()]['PatientID'].nunique()

percent_malignant = (malignant / unique_patients) * 100
percent_benign = (benign / unique_patients) * 100
percent_none = (none / unique_patients) * 100


# Biopsy Laterality comparison (RIGHT vs LEFT)
biop_laterality_counts = data['Biop_Laterality'].value_counts()

total_biop_laterality = biop_laterality_counts.sum()  # total non-null biop_laterality records

right_biopsies = biop_laterality_counts.get('RIGHT', 0)
left_biopsies = biop_laterality_counts.get('LEFT', 0)

percent_right = (right_biopsies / total_biop_laterality) * 100 if total_biop_laterality > 0 else 0
percent_left = (left_biopsies / total_biop_laterality) * 100 if total_biop_laterality > 0 else 0


# Average age (only for unique patients)
average_age = data.drop_duplicates(subset=['PatientID'])['Age'].mean()

# Gender distribution (only for unique patients)
gender_distribution = data.drop_duplicates(subset=['PatientID'])['Gender'].value_counts(normalize=True) * 100

# BI-RADS distribution
birads_distribution = data['BI-RADS'].value_counts(normalize=True) * 100  # percentage distribution

# Exam Laterality distribution
exam_laterality_distribution = data['Exam_Laterality'].value_counts(normalize=True) * 100  # percentage distribution


# Print out the statistics
print(f"Total unique patients: {unique_patients}")
print(f"Total unique accessions: {unique_accessions}")
print(f"Average accessions per patient: {average_accessions_per_patient:.2f}")

print(f"\nPercentage of unique patients with Malignant pathology: {percent_malignant:.2f}%")
print(f"Percentage of unique patients with Benign pathology: {percent_benign:.2f}%")
print(f"Percentage of unique patients with no pathology data: {percent_none:.2f}%")
print(f"Average age of unique patients: {average_age:.2f}")

print("\nGender distribution among unique patients:")
print(gender_distribution)

print("\nBiopsy Laterality:")
print(f"Percentage of biopsies on the RIGHT side: {percent_right:.2f}%")
print(f"Percentage of biopsies on the LEFT side: {percent_left:.2f}%")

print("\nBI-RADS distribution:")
for birads_category, percentage in birads_distribution.items():
    print(f"BI-RADS {birads_category}: {percentage:.2f}%")
    
print("\nExam Laterality distribution:")
for exam_laterality, percentage in exam_laterality_distribution.items():
    print(f"{exam_laterality}: {percentage:.2f}%")