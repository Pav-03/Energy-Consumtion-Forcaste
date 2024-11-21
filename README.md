# Energy-Consumtion-Forcaste

## Project Introduction

I am currently working on an energy consumption forecasting project for Denmark, predicting energy demand for the next 24 hours. This project leverages advanced machine learning techniques to accurately forecast usage patterns, contributing to better resource planning and energy distribution. Previously, I worked on similar projects in India, gaining hands-on experience with forecasting models. In this iteration, I am exploring new tools and methodologies, utilizing FTI structure for the ML pipeline, Hopsworks as the feature store, and deploying the solution on Google Cloud Platform (GCP) to optimize scalability and performance.


### Architecture Overview: Adopting the FTI Structure

This project follows the FTI (Feature, Training, Inference) architecture pattern, a modular and scalable framework for building machine learning systems. The FTI structure has been integrated into this project to ensure clarity, scalability, and maintainability.

Why Use the FTI Architecture?
The FTI architecture was chosen for its numerous benefits in structuring machine learning systems:

1) Modularity and Separation of Concerns:
Each pipeline (Feature, Training, and Inference) has a distinct responsibility, making the system easier to understand and manage.

2) Flexibility in Development:
Each pipeline can use a different tech stack or framework, allowing us to tailor solutions to the specific requirements of the project.

3) Scalability:
The pipelines can be independently scaled and monitored, which aligns with the need for a robust production-ready system.

4) Traceability and Versioning:
By using a Feature Store and a Model Registry, the architecture ensures version control and reproducibility of both features and models. 

This mitigates risks such as training-serving skew and makes model updates seamless.
For more Information: https://www.hopsworks.ai/post/mlops-to-ml-systems-with-fti-pipelines
