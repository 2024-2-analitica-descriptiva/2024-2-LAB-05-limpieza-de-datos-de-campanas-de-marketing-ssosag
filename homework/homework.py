"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import pandas as pd
    import zipfile
    import os
    import glob

    def loadData(inputPath):
        data = pd.DataFrame()

        files = glob.glob(f"{inputPath}/*")
        for file in files:
            with zipfile.ZipFile(file, "r") as z:
                df = pd.read_csv(z.open(z.namelist()[0]), index_col=0)
                data = pd.concat([data, df])

        data.set_index("client_id", inplace=True)

        return data

    def cleanData(df):
        df["job"] = df["job"].str.replace(".", "").str.replace("-", "_")
        df["education"] = (
            df["education"].str.replace(".", "_").replace("unknown", pd.NA)
        )
        df["credit_default"] = df["credit_default"].apply(
            lambda x: 1 if x == "yes" else 0
        )
        df["mortgage"] = df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

        df["previous_outcome"] = df["previous_outcome"].apply(
            lambda x: 1 if x == "success" else 0
        )

        df["campaign_outcome"] = df["campaign_outcome"].apply(
            lambda x: 1 if x == "yes" else 0
        )

        MONTHMAPPING = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }

        df["month"] = df["month"].map(MONTHMAPPING)

        df["last_contact_date"] = pd.to_datetime(df[["day", "month"]].assign(year=2022))

        return df

    def createOutputDirectory(outputPath):
        if os.path.exists(outputPath):
            for file in glob.glob(f"{outputPath}/*"):
                os.remove(file)
            os.rmdir(outputPath)
        os.makedirs(outputPath)

    def csvClient(df, outputPath):
        dfClient = df[
            ["age", "job", "marital", "education", "credit_default", "mortgage"]
        ]

        dfClient.to_csv(outputPath + "/client.csv")

    def csvCampaign(df, outputPath):
        dfCampaign = df[
            [
                "number_contacts",
                "contact_duration",
                "previous_campaign_contacts",
                "previous_outcome",
                "campaign_outcome",
                "last_contact_date",
            ]
        ]
        dfCampaign.to_csv(outputPath + "/campaign.csv")

    def csvEconomics(df, outputPath):
        dfEconomics = df[["cons_price_idx", "euribor_three_months"]]
        dfEconomics.to_csv(outputPath + "/economics.csv")

    inputPath = "files/input"
    outputPath = "files/output"
    df = loadData(inputPath)
    dfClean = cleanData(df)

    createOutputDirectory(outputPath)
    csvClient(dfClean, outputPath)
    csvCampaign(dfClean, outputPath)
    csvEconomics(dfClean, outputPath)

    return


if __name__ == "__main__":
    clean_campaign_data()
