# Web Interfaces Access Guide

All services are now running with web interfaces! Here's how to access them.

## 🌐 Available Interfaces

### 1. Kafka UI (Port 8090)
**URL:** http://localhost:8090

**What you can do:**
- ✅ View all Kafka topics
- ✅ Browse messages in topics
- ✅ Monitor consumer groups
- ✅ View broker information
- ✅ Create/delete topics
- ✅ Publish test messages
- ✅ Monitor cluster health

**Quick Start:**
1. Open http://localhost:8090 in your browser
2. Click "Topics" to see: `esport-matches`, `esport-players`, `esport-rankings`
3. Click on a topic to view messages
4. Use "Produce Message" to send test data

---

### 2. Spark Master UI (Port 8080)
**URL:** http://localhost:8080

**What you can do:**
- ✅ View cluster status
- ✅ Monitor running applications
- ✅ Check worker nodes
- ✅ View application history
- ✅ Monitor resource usage (cores, memory)
- ✅ Access application UIs

**Quick Info:**
- **Master URL:** spark://spark-master:7077
- **Workers:** 1 worker with 2 cores, 2GB memory
- **Status:** ALIVE

---

### 3. Spark Worker UI (Port 8081)
**URL:** http://localhost:8081

**What you can do:**
- ✅ View worker status
- ✅ Monitor executors
- ✅ Check resource usage
- ✅ View logs

---

## 🔧 GitHub Codespaces Port Forwarding

If you're using GitHub Codespaces, ports are automatically forwarded. Access them via:

### Method 1: Ports Panel
1. Click "PORTS" tab in VS Code bottom panel
2. Find your port (8090, 8080, 8081)
3. Click the 🌐 icon or right-click → "Open in Browser"

### Method 2: Auto-Generated URLs
Codespaces creates URLs like:
```
https://<codespace-name>-8090.app.github.dev  # Kafka UI
https://<codespace-name>-8080.app.github.dev  # Spark Master
https://<codespace-name>-8081.app.github.dev  # Spark Worker
```

### Method 3: Port Panel Actions
- **Public:** Make port accessible to anyone
- **Private:** Only you can access
- **Label:** Add custom name for easy identification

---

## 📊 Quick Commands

### Check Service Status
```bash
docker ps
```

### View Logs
```bash
# Kafka UI logs
docker logs kafka-ui -f

# Spark Master logs
docker logs spark-master -f

# Spark Worker logs
docker logs spark-worker -f

# Kafka logs
docker logs kafka -f
```

### Restart Services
```bash
# Restart all services
docker compose --profile core --profile spark restart

# Restart specific service
docker restart kafka-ui
docker restart spark-master
```

---

## 🎯 Common Tasks

### Task 1: View Kafka Messages via UI

1. Open http://localhost:8090
2. Navigate to "Topics" → "esport-matches"
3. Click "Messages" tab
4. Set "Offset" to "Earliest"
5. Click "Submit"
6. Browse your messages with full JSON formatting

### Task 2: Publish Test Message via UI

1. Open Kafka UI (http://localhost:8090)
2. Go to "Topics" → "esport-matches"
3. Click "Produce Message" button
4. Choose "JSON" format
5. Paste test data:
```json
{
  "matchId": "TEST_001",
  "gameMode": "CLASSIC",
  "gameDuration": 1800,
  "timestamp": 1735654800000
}
```
6. Click "Produce Message"

### Task 3: Monitor Spark Applications

1. Open Spark Master UI (http://localhost:8080)
2. View "Running Applications" section
3. Click on an application to see details
4. Check "Executors" tab for resource usage
5. View "SQL" tab for query plans (if using Spark SQL)

### Task 4: Submit Spark Job and Monitor

```bash
# Submit a Spark job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /workspace/src/streaming/jobs/streaming_job_template.py

# Then check http://localhost:8080 to see it running
# Click on the app to see live metrics
```

---

## 🚨 Troubleshooting

### Issue: "Cannot access http://localhost:8090"

**If running locally:**
- Ensure Docker containers are running: `docker ps`
- Check if port is bound: `netstat -an | grep 8090` or `lsof -i :8090`
- Try http://127.0.0.1:8090 instead

**If in GitHub Codespaces:**
- Check PORTS tab in VS Code
- Port should show as "Forwarded"
- Use the Codespaces-provided URL (not localhost)
- Click 🌐 icon in PORTS panel

### Issue: "Kafka UI shows no topics"

**Solution:**
```bash
# Wait 10-15 seconds after starting Kafka
sleep 15

# Verify topics exist
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Restart Kafka UI
docker restart kafka-ui
```

### Issue: "Spark UI shows no workers"

**Solution:**
```bash
# Check if worker is running
docker logs spark-worker

# Restart worker
docker restart spark-worker

# Check Master logs
docker logs spark-master | grep -i worker
```

### Issue: Port forwarding not working in Codespaces

**Solution:**
1. Open Command Palette (Ctrl+Shift+P / Cmd+Shift+P)
2. Type "Ports: Focus on Ports View"
3. Right-click on port → "Port Visibility" → "Public"
4. Copy the forwarded address
5. Open in new browser tab

---

## 🎨 Kafka UI Features Deep Dive

### Topic Operations
- **Create Topic:** Topics → Create Topic → Set partitions/replication
- **Delete Topic:** Topics → [Topic Name] → Actions → Delete
- **Purge Topic:** Remove all messages from a topic
- **Edit Config:** Modify retention, cleanup policy, etc.

### Message Operations
- **Browse Messages:** View with filters, search, JSON formatting
- **Produce Message:** Send test data in JSON/String/Avro format
- **Download Messages:** Export messages to file
- **Jump to Offset:** Go to specific message position

### Consumer Groups
- **View Active Consumers:** See who's reading your topics
- **Monitor Lag:** Check if consumers are keeping up
- **Reset Offsets:** Replay messages from specific position

### Cluster Health
- **Broker Status:** CPU, memory, disk usage
- **Topic Metrics:** Messages/sec, bytes/sec
- **Partitions:** Leader, replicas, in-sync replicas

---

## 📈 Spark UI Features Deep Dive

### Master UI (8080)
- **Workers:** View all worker nodes and their resources
- **Applications:** Running, completed, and failed apps
- **Resource Usage:** Cores used, memory allocated
- **Application Details:** Click app → detailed metrics

### Worker UI (8081)
- **Executors:** Running executors on this worker
- **Logs:** stdout/stderr for debugging
- **Resources:** CPU, memory usage per executor

### Application UI (when running)
- **Jobs:** Spark jobs in the application
- **Stages:** Tasks within each job
- **Storage:** Cached RDDs/DataFrames
- **Environment:** Spark configuration
- **Executors:** Resource usage per executor
- **SQL:** Query plans and metrics (for Spark SQL)

---

## ✅ Verification Checklist

Run these commands to verify everything is working:

```bash
# 1. Check all containers are running
docker ps

# Expected: 5 containers running
# - kafka-ui
# - kafka
# - spark-master
# - spark-worker
# - zookeeper

# 2. Check Kafka UI is responding
curl -s http://localhost:8090 | head -5

# Expected: HTML content

# 3. Check Spark Master UI is responding
curl -s http://localhost:8080 | head -5

# Expected: HTML content

# 4. List Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Expected:
# esport-matches
# esport-players
# esport-rankings

# 5. Check Spark cluster
docker logs spark-master | grep -i "started"

# Expected: "Started Master" message
```

---

## 🎯 Next Steps

Now that you have UI access:

1. **Explore Kafka UI:**
   - View the 3 topics created
   - Produce some test messages
   - Watch them appear in real-time

2. **Monitor Spark:**
   - Observe the worker connecting to master
   - Check resource allocation

3. **Run the Producer:**
   ```bash
   PYTHONPATH=$PWD python src/ingestion/riot_producer.py
   ```
   Then watch messages flow into Kafka UI!

4. **Submit Spark Jobs:**
   - Deploy streaming jobs
   - Monitor them in Spark UI
   - View execution metrics

---

## 🔗 Quick Links Summary

| Service | Port | Local URL | Purpose |
|---------|------|-----------|---------|
| Kafka UI | 8090 | http://localhost:8090 | Kafka management & monitoring |
| Spark Master | 8080 | http://localhost:8080 | Spark cluster status |
| Spark Worker | 8081 | http://localhost:8081 | Worker node details |
| Kafka Broker | 9092 | localhost:9092 | Kafka connections (CLI/code) |
| Zookeeper | 2181 | localhost:2181 | Kafka coordination (CLI) |

---

**Enjoy exploring your Big Data platform! 🎉**
