# MongoDB Connection Troubleshooting Guide

## Problem
The API endpoint `/api/uploader/reports/formulas/all` is returning:
```json
{
    "error": "MongoDB connection error: MongoDB is not connected",
    "status_code": 503,
    "endpoint": "/api/uploader/reports/formulas/all"
}
```

## Root Cause
The application cannot connect to MongoDB. This could be due to:
1. MongoDB service not running
2. Incorrect MongoDB configuration
3. Network connectivity issues
4. Authentication/authorization problems
5. MongoDB service crashed or stopped

---

## Manual Steps to Resolve

### Step 1: Check MongoDB Service Status

SSH into your server and check if MongoDB is running:

```bash
# Check if MongoDB service is running
sudo systemctl status mongod

# Or if using MongoDB Community Edition
sudo systemctl status mongodb

# Alternative: Check if MongoDB process is running
ps aux | grep mongod
```

**Expected Output:**
- Service should show `active (running)`
- Process should be visible in `ps aux`

**If not running, proceed to Step 2.**

---

### Step 2: Start MongoDB Service

If MongoDB is not running, start it:

```bash
# Start MongoDB service
sudo systemctl start mongod

# Or
sudo systemctl start mongodb

# Enable MongoDB to start on boot
sudo systemctl enable mongod

# Verify it started successfully
sudo systemctl status mongod
```

---

### Step 3: Check MongoDB Configuration

Check the MongoDB configuration file:

```bash
# Check MongoDB config file location
# Usually one of these:
cat /etc/mongod.conf
# OR
cat /etc/mongodb.conf

# Check if MongoDB is listening on the correct port
sudo netstat -tlnp | grep 27017
# OR
sudo ss -tlnp | grep 27017
```

**Verify:**
- MongoDB should be listening on port `27017` (default)
- If using a different port, update application configuration

---

### Step 4: Check MongoDB Logs

Check MongoDB logs for errors:

```bash
# Check MongoDB logs
sudo tail -n 100 /var/log/mongodb/mongod.log

# Or if logs are in a different location
sudo journalctl -u mongod -n 100

# Look for errors like:
# - Connection refused
# - Permission denied
# - Port already in use
# - Disk space issues
```

**Common Issues:**
- **Port already in use**: Another process is using port 27017
- **Permission denied**: MongoDB doesn't have access to data directory
- **Disk full**: No space left on device

---

### Step 5: Verify MongoDB Connection from Server

Test MongoDB connection directly from the server:

```bash
# Test MongoDB connection (if MongoDB client is installed)
mongosh
# OR
mongo

# If connection works, you should see MongoDB shell prompt
# Type: exit to quit

# Alternative: Test connection using telnet
telnet localhost 27017

# Or using nc (netcat)
nc -zv localhost 27017
```

**Expected Output:**
- MongoDB shell should connect successfully
- `telnet`/`nc` should show connection established

---

### Step 6: Check Application Configuration

Verify MongoDB configuration in your application:

```bash
# Navigate to your application directory
cd /path/to/LB-Uploader

# Check environment variables
env | grep -i mongo

# Check properties file (if using staging profile)
cat application-stage.properties | grep -i mongo

# Check if MongoDB config is set
# Should see:
# mongo.host=...
# mongo.port=...
# mongo.database=...
# mongo.username=... (if using authentication)
# mongo.password=... (if using authentication)
```

**Configuration to verify:**
- `mongo.host` - Should be `localhost` or MongoDB server IP
- `mongo.port` - Should be `27017` (or your MongoDB port)
- `mongo.database` - Should be `devyani_mongo`
- `mongo.username` and `mongo.password` - Only if MongoDB requires authentication

---

### Step 7: Check MongoDB Authentication (if enabled)

If MongoDB has authentication enabled:

```bash
# Connect to MongoDB
mongosh

# Or with authentication
mongosh -u <username> -p <password> --authenticationDatabase admin

# Test connection
use admin
db.auth("<username>", "<password>")

# Check if user has access to the database
use devyani_mongo
show collections
```

**If authentication fails:**
- Verify username/password in application configuration
- Check if user has permissions on `devyani_mongo` database
- Verify `authSource` is set correctly (usually `admin`)

---

### Step 8: Check Network and Firewall

If MongoDB is on a different server:

```bash
# Test network connectivity to MongoDB server
ping <mongodb-host>

# Test port connectivity
telnet <mongodb-host> 27017
# OR
nc -zv <mongodb-host> 27017

# Check firewall rules
sudo ufw status
# OR
sudo iptables -L -n | grep 27017

# If firewall is blocking, allow MongoDB port
sudo ufw allow 27017/tcp
```

---

### Step 9: Restart Application Service

After fixing MongoDB, restart your application:

```bash
# If using systemd service
sudo systemctl restart <your-app-service-name>

# Or if using PM2
pm2 restart <app-name>

# Or if using supervisor
sudo supervisorctl restart <app-name>

# Or manually (if running in screen/tmux)
# Find the process and restart it
ps aux | grep python
# Kill and restart
```

---

### Step 10: Verify Connection After Restart

Test the API endpoint again:

```bash
# Test the health endpoint (if available)
curl https://reconciistageuploader.corepeelers.com/api/health

# Test the problematic endpoint
curl https://reconciistageuploader.corepeelers.com/api/uploader/reports/formulas/all

# Check application logs
tail -f /path/to/application/logs/app.log
# OR
journalctl -u <your-app-service> -f
```

**Expected Output:**
- Health endpoint should show MongoDB as "connected"
- API endpoint should return data instead of 503 error
- Logs should show "✅ MongoDB connection established successfully"

---

## Quick Fix Commands (Summary)

If you just need to quickly restart everything:

```bash
# 1. Restart MongoDB
sudo systemctl restart mongod

# 2. Verify MongoDB is running
sudo systemctl status mongod

# 3. Test MongoDB connection
mongosh --eval "db.adminCommand('ping')"

# 4. Restart your application
sudo systemctl restart <your-app-service>
# OR
pm2 restart all

# 5. Check application logs
tail -f /path/to/logs/app.log
```

---

## Common Issues and Solutions

### Issue 1: MongoDB Service Won't Start

**Symptoms:** `systemctl status mongod` shows `failed` or `inactive`

**Solutions:**
```bash
# Check logs for specific error
sudo journalctl -u mongod -n 50

# Check if data directory exists and has correct permissions
sudo ls -la /var/lib/mongodb
sudo chown -R mongodb:mongodb /var/lib/mongodb

# Check disk space
df -h

# Try starting with verbose logging
sudo mongod --config /etc/mongod.conf --verbose
```

### Issue 2: Port 27017 Already in Use

**Symptoms:** MongoDB fails to start, port conflict error

**Solutions:**
```bash
# Find what's using the port
sudo lsof -i :27017
# OR
sudo netstat -tlnp | grep 27017

# Kill the process if it's not MongoDB
sudo kill -9 <PID>

# Or change MongoDB port in /etc/mongod.conf
# Then update application configuration
```

### Issue 3: Authentication Failed

**Symptoms:** Connection works but authentication fails

**Solutions:**
```bash
# Create/update MongoDB user
mongosh
use admin
db.createUser({
  user: "your_username",
  pwd: "your_password",
  roles: [{ role: "readWrite", db: "devyani_mongo" }]
})

# Update application configuration with correct credentials
```

### Issue 4: Connection Timeout

**Symptoms:** Connection attempts timeout

**Solutions:**
```bash
# Check MongoDB is listening on correct interface
# Edit /etc/mongod.conf
# Set: bindIp: 0.0.0.0 (or specific IP)
# Restart MongoDB

# Check firewall
sudo ufw allow 27017/tcp

# Check network connectivity
ping <mongodb-host>
```

---

## Prevention

To prevent this issue in the future:

1. **Enable MongoDB auto-start:**
   ```bash
   sudo systemctl enable mongod
   ```

2. **Set up monitoring:**
   - Monitor MongoDB service status
   - Set up alerts for connection failures
   - Monitor disk space

3. **Regular backups:**
   - Backup MongoDB data regularly
   - Test restore procedures

4. **Health checks:**
   - Use the `/api/health` endpoint for monitoring
   - Set up automated health checks

---

## Additional Resources

- MongoDB Documentation: https://docs.mongodb.com/
- MongoDB Troubleshooting: https://docs.mongodb.com/manual/administration/troubleshooting/
- Systemd Service Management: https://www.digitalocean.com/community/tutorials/how-to-use-systemctl-to-manage-systemd-services-and-units

---

## Contact

If the issue persists after following these steps:
1. Check application logs for specific error messages
2. Check MongoDB logs for connection attempts
3. Verify all configuration values are correct
4. Contact your system administrator or DevOps team

