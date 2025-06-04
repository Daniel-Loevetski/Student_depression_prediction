# Big Data Analysis: Real-Time Depression Prediction with Spark & Kafka

This project demonstrates a real-time student depression prediction pipeline using Apache Spark (Structured Streaming) and Apache Kafka. It includes data ingestion, streaming, and machine learning model inference for predicting depression levels based on student data.

---

## Repository Structure

```
Submission/
├── Final Project.ipynb
├── Kafka Producer.ipynb
├── Student_Depression_Dataset.csv
└── Synthetic_Students_for_Streaming.csv
```

- **Final Project.ipynb**  
  Main Jupyter notebook implementing:
  1. Batch data processing and model training for depression prediction (Parts 1 & 2).
  2. Spark Structured Streaming code for real-time inference (Part 3).
  3. Live visualization of streaming predictions.

- **Kafka Producer.ipynb**  
  Jupyter notebook containing the Kafka producer logic. It reads `Synthetic_Students_for_Streaming.csv` and streams records to a Kafka topic at a configurable interval.

- **Student_Depression_Dataset.csv**  
  Real student depression dataset used for batch model training (covering features like demographic, lifestyle, and mental health indicators).

- **Synthetic_Students_for_Streaming.csv**  
  Synthetic student records generated to simulate real-time data streaming for Part 3 demonstrations.

---

## Prerequisites

- **Operating System**: Linux (tested on Ubuntu-derived VLab environment).
- **Apache Spark**: v3.x with Kafka integration (`spark-sql-kafka-0-10` package).
- **Apache Kafka**: v2.x or v3.x (installed under `/usr/local/kafka/`).
- **Python 3.x** and **Jupyter Notebook**.
- Internet connection (for downloading Spark packages if not already installed).

---

## Setup Instructions

1. **Clone or Extract the Submission Folder**  
   Place the entire `Submission` directory on your Linux machine (e.g., under `~/Desktop/Submission`).

2. **Launch Spark with Kafka Package**  
   Open a terminal in the `Submission` folder and start a PySpark session with the Kafka connector:
   ```bash
   pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1
   ```
   This session is used to run the batch parts (Parts 1 & 2) in `Final Project.ipynb`.

3. **Start Apache Kafka (for Real-Time Streaming: Part 3)**  
   Open three separate terminal windows and run the following commands:

   - **Terminal 1 (Zookeeper)**  
     ```bash
     cd /usr/local/kafka/kafka_2.13-3.2.1
     bin/zookeeper-server-start.sh config/zookeeper.properties
     ```

   - **Terminal 2 (Kafka Broker)**  
     ```bash
     cd /usr/local/kafka/kafka_2.13-3.2.1
     bin/kafka-server-start.sh config/server.properties
     ```

   - **Terminal 3 (Create Topic and Run Producer)**  
     ```bash
     cd /usr/local/kafka/kafka_2.13-3.2.1
     # Create a topic named "student_depression"
     bin/kafka-topics.sh --create --topic student_depression --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
     ```
     Later, you will run the producer code inside `Kafka Producer.ipynb` to send synthetic data to this topic.

---

## Usage

### Part 1 & 2: Batch Model Training (Final Project.ipynb)

1. **Open Jupyter Notebook**  
   Launch Jupyter:
   ```bash
   jupyter notebook
   ```
   Navigate to the `Submission` folder and open `Final Project.ipynb`.

2. **Run Batch Cells**  
   - **Data Loading & Preprocessing**  
     Load `Student_Depression_Dataset.csv`, perform cleaning, feature engineering, and split into training/testing sets.
   - **Model Training & Evaluation**  
     Train a Spark ML pipeline (e.g., logistic regression, feature selection), evaluate performance, and save the trained model to disk or in-memory for streaming.

3. **Save Trained Pipeline (Optional)**  
   If you need to persist the trained pipeline for reuse in streaming, follow the notebook instructions to save and load the Spark ML model.

### Part 3: Real-Time Streaming Inference

1. **Ensure Kafka and Zookeeper Are Running**  
   As described in the setup, make sure both Zookeeper (Terminal 1) and Kafka broker (Terminal 2) are active.

2. **Run Kafka Producer**  
   - Open `Kafka Producer.ipynb` in Jupyter.
   - Update the Kafka broker endpoint (`localhost:9092`) and topic name (`student_depression`) if needed.
   - Execute the cells to read `Synthetic_Students_for_Streaming.csv` and stream one record at a time to the `student_depression` topic.

3. **Run Streaming Consumer & Inference**  
   - In `Final Project.ipynb`, locate the streaming section (Part 3).
   - The notebook subscribes to the `student_depression` topic, deserializes each message, applies the trained ML model to predict depression score in real time, and outputs results to the notebook (and optionally to a sink like console or memory).
   - Execute all streaming cells; you should see live prediction outputs as the synthetic data flows in.

4. **Live Visualization**  
   - The notebook includes code to collect streaming predictions and generate live-updating charts (e.g., scatter plots or line graphs) to visualize model outputs over time.
   - Make sure to run cells in order to initialize any required buffers or streaming queries for visualization.

---

## Dataset Descriptions

- **Student_Depression_Dataset.csv**  
  - Contains real student survey data.  
  - Fields include demographic information (age, gender), academic factors, lifestyle indicators (sleep, diet, exercise), and mental health scores.  
  - Used for training and evaluating the depression prediction model.

- **Synthetic_Students_for_Streaming.csv**  
  - A smaller, synthetically generated dataset that mimics the schema of `Student_Depression_Dataset.csv`.  
  - Used in `Kafka Producer.ipynb` to simulate real-time events for streaming prediction.

---

## Dependencies

- **Apache Spark** (with PySpark)  
- **spark-sql-kafka-0-10_2.12** connector (version matching your Spark deployment; here: 3.2.1)
- **Apache Kafka** (2.x or 3.x)
- **Python 3.x**  
- **Jupyter Notebook**  
- **pandas**, **numpy**, **matplotlib**, **seaborn** (for data handling and visualization)
- **pyspark** (for Spark DataFrame and ML APIs)

Install any missing Python packages via:
```bash
pip install pandas numpy matplotlib seaborn
```

---

## Troubleshooting

- **Spark-Packages Download Issues**  
  If Spark fails to download the Kafka connector, ensure your internet connection is active or manually place the JAR in `SPARK_HOME/jars/`.

- **Kafka Connection Failures**  
  - Verify Zookeeper and Kafka are running on `localhost:9092`.  
  - Check that the topic `student_depression` exists by running:
    ```bash
    bin/kafka-topics.sh --list --bootstrap-server localhost:9092
    ```

- **Notebook Kernel Crashes**  
  - Ensure your machine has sufficient memory and CPU resources.  
  - If using VLab, confirm you launched PySpark with the correct dependencies (`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1`).

---

## Acknowledgements

- Project developed as part of the **Big Data Analysis** course.
- Thanks to the college’s VLab team for providing a Spark + Kafka environment.

---

*Enjoy exploring real-time depression prediction with Big Data tools!*
