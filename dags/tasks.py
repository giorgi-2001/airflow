from sqlalchemy import create_engine
from rdkit.Chem import AllChem, Descriptors
import pandas as pd
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


INPUT_PATH = "./dags/temp/mols.csv"
OUTPUT_PATH = "./dags/temp/lipinski_mols.xlsx"
AWS_S3_BUCKET_NAME = "giorgi-2001-hw-bucket"


def connect():
    pg_hook = PostgresHook("db_connection")
    engine = pg_hook.get_conn()
    query = "SELECT * FROM molecules"
    mols_df = pd.read_sql(
        sql=query,
        con=engine
    )

    mols_df.to_csv(INPUT_PATH, index=False)


def process():
    mols_df = pd.read_csv(INPUT_PATH, index_col="id")
    mols_df.sort_index(ascending=True, inplace=True)

    mols_df["mols"] = mols_df["smiles"].apply(
        lambda smiles: AllChem.MolFromSmiles(smiles)
    )

    mol_props_funcs = {
        'Molecular weight': lambda mol: Descriptors.MolWt(mol),
        'logP': lambda mol: Descriptors.MolLogP(mol),
        "TPSA": lambda mol: Descriptors.TPSA(mol),
        'H Acceptors': lambda mol: Descriptors.NumHAcceptors(mol),
        'H Donors': lambda mol: Descriptors.NumHDonors(mol),
    }

    mol_props_to_compute = list(mol_props_funcs.keys())
    mols_df[mol_props_to_compute] = mols_df.apply(
        lambda row: [mol_props_funcs[prop](row['mols']) for prop in mol_props_to_compute],
        axis=1,
        result_type='expand'
    )

    lipinski_pass = (
        (mols_df['Molecular weight'] < 500)
        & (mols_df['logP'] < 5)
        & (mols_df['H Acceptors'] < 10)
        & (mols_df['H Donors'] < 5)
    )

    mols_df["Lipinski pass"] = lipinski_pass

    mols_df.to_excel(OUTPUT_PATH)


def upload():
    now = datetime.now()
    date = str(pd.Timestamp(now).floor("S")).replace(" ", "_").replace(":", "-")
    obj_name = "mols/" + date + ".xlsx"

    try:
        s3 = S3Hook("S3_connection")
        s3.load_file(
            filename=OUTPUT_PATH,
            key=obj_name,
            bucket_name=AWS_S3_BUCKET_NAME,
            replace=True
        )
        print(
            f"File '{OUTPUT_PATH}' uploaded to "
            f"'{AWS_S3_BUCKET_NAME}/{obj_name}' successfully."
        )
    except FileNotFoundError:
        print(f"The file '{OUTPUT_PATH}' was not found.")

    
