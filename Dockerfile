FROM continuumio/miniconda3:latest

WORKDIR /app

# Copy environment.yml
COPY environment.yml .

# Create conda environment
RUN conda env create -f environment.yml

# Ensure Bash shell for conda
SHELL ["/bin/bash", "-c"]

# Add conda environment python to PATH
ENV PATH /opt/conda/envs/MLB_Betting/bin:$PATH

# Copy project files
COPY src/ ./src/
COPY predictionsApp/ ./predictionsApp/
COPY databases/ ./databases/

# Expose Flask port
EXPOSE 5000

# Run Flask app using the MLB_Betting environment
CMD ["python", "predictionsApp/app.py"]