# Use Node.js LTS version
FROM node:18

# Set working directory
WORKDIR /app

# Copy frontend files to container
COPY . .

# Install dependencies
RUN npm install

# Build the Next.js app
RUN npm run build

# Expose the frontend port
EXPOSE 3000

# Start the Next.js app
CMD ["npm", "run", "start"]
