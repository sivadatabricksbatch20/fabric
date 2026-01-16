# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d9ebad35-6987-4924-8dbf-677847936bf0",
# META       "default_lakehouse_name": "first_lakehoue",
# META       "default_lakehouse_workspace_id": "9ec5a999-67d7-41a8-8979-6c8ebda21878",
# META       "known_lakehouses": [
# META         {
# META           "id": "d9ebad35-6987-4924-8dbf-677847936bf0"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

a=4
b=5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.ls("Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mkdirs("Files/target_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.cp("Files/test_data/Financial Sample.xlsx","Files/target_data/Financial Sample.xlsx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.run("notebook_for_testing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.runMultiple(["Notebook_for_testing " , "Notebook_for _testing2","Notebook_for_testing3"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

dag = {
    "activities": [
        {
            "name": "Notebook_for_testing",
            "path": "Notebook_for_testing",
            "dependencies": []
        },
        {
            "name": "Notebook_for_testing2",
            "path": "Notebook_for_testing2",
            "dependencies": ["Notebook_for_testing3"]
        },
        {
            "name": "Notebook_for_testing3",
            "path": "Notebook_for_testing3",
            "dependencies": []
        }
    ]
}

notebookutils.notebook.runMultiple(dag,{"displayDAGViaGraphviz":True})



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.mkdirs('new directory name')  
notebookutils.fs.mkdirs("Files/<new_dir>")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.rm('file path', True) # Set the last parameter as True to remove all files and directories recursively

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.exists(file: String) # Boolean -> Check if a file or directory exists

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# works with the default lakehouse files using relative path 
notebookutils.fs.ls("Files/tmp") 
# based on ABFS file system 
notebookutils.fs.ls("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>")  
# based on local file system of driver node
notebookutils.fs.ls("file:/tmp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Moves a file or directory to a abfss directory.
notebookutils.fs.mv("/local/path", "abfss://CONTAINER.dfs.core.windows.net")
# Moves a file or directory to a new directory, overwrite the destination folder if exists.
notebookutils.fs.mv("/mnt/remote/path", "/local/path", overwrite=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.head('file path', maxBytes to read)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.put("file path", "content to write")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.lakehouse.getWithProperties("artifact_name", "optional_workspace_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List all tables in a Lakehouse
notebookutils.lakehouse.list(workspaceId={workspaceId}, maxResults)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Starts a load table operation in a Lakehouse artifact
notebookutils.lakehouse.loadTable(
    {
        "relativePath": "Files/myFile.csv",
        "pathType": "File",
        "mode": "Overwrite",
        "recursive": False,
        "formatOptions": {
            "format": "Csv",
            "header": True,
            "delimiter": ","
        }
    }, table_name, artifact_name, workspaceId={workspaceId})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get information about a Lakehouse in the current workspace, maximum number of Notebooks to return default is 1000
notebookutils.notebook.list()
# Get information about a Lakehouse in another workspace, maximum number of Notebooks to return is 100
notebookutils.notebook.list(workspaceId={workspaceId},maxResults=100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get a notebook in the current workspace
notebookutils.notebook.get(name)
# Get a notebook in another workspace
notebookutils.notebook.get(name, workspaceId={workspaceId})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
# List file system utility APIs
notebookutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
# Reference a notebook and returns its exit value. You can run nesting function calls in a notebook interactively or in a pipeline.
notebookutils.notebook.run("NotebookName")
# Set timeout of each cell to 200s and add a argument for child notebook.
# notebookutils.notebook.run("NotebookName", 200, {"input": 20})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Azure Key Vault does not support cross-tenant secret access using managed identity. Fabric notebooks authenticate using the workspace managed identity, so the Key Vault must be created in the same Entra ID tenant.

# CELL ********************

notebookutils.credentials.getSecret('https://keys243.vault.azure.net/', 'sqlpassword')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get information about a Lakehouse in the current workspace, maximum number of Notebooks to return default is 1000
notebookutils.notebook.list()
# Get information about a Lakehouse in another workspace, maximum number of Notebooks to return is 100
notebookutils.notebook.list(workspaceId={workspaceId},maxResults=100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
