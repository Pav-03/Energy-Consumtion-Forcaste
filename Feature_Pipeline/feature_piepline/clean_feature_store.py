import fire 
import hopsworks

from feature_piepline import settings


def clean():

    """
    Utility functiosn used during development to clean all the data from the feature view.
    
    Basically you dont want to clean the hopsworks space but  i am using free tier vesion which allow me 100 feature view so
    just trying to stay in free verion.
    
    """
    project = hopsworks.login(
        api_key_value=settings.SETTINGS["FS_API_KEY"],
        project=settings.SETTINGS["FS_PROJECT_NAME"],
    )

    fs = project.get_feature_store()

    print("Deleting feature views and training datasets")
    try:
        feature_views = fs.get_feature_view(name="energy_consumption_denmak_view")

        for feature_view in feature_views:
            try:
                feature_view.delete()
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)

    
    print("deleting feature groups")
    try:
        feature_groups = fs.get_feature_groups(name="energy_cosumption_denmark")
        for feature_group in feature_groups:
            try:
                feature_group.delete()
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    fire.Fire(clean)
    