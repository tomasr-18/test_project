0. Skapa en branch för micro-servicen
1. skapa följande filer:
    Dockerfile
    cloudbuild.yaml
2.  Skapa ett repo i Artifact repo i GCP
3.  Skapa en trigger under Cloud Run som triggas av updateringar på din branch
4.  Skapa en service under Cloud Run med den imagen som skapats i Cloud Run. imagen ska ligga i det Artifact Repo du precis skapade.
