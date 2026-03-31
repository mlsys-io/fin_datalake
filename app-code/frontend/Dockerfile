# =============================================================================
# AI Lakehouse Frontend - Multi-stage Build
# =============================================================================

# Step 1: Build the React Application
FROM node:22-alpine AS build-stage
WORKDIR /app

# Install dependencies (only package.json first for caching)
COPY package*.json ./
RUN npm install

# Copy source and build
COPY . .
RUN npm run build

# Step 2: Production-ready Nginx (Alpine)
FROM nginx:alpine AS production-stage

# Copy the build output from the build-stage
COPY --from=build-stage /app/dist /usr/share/nginx/html

# Default nginx.conf is overridden by K8s ConfigMap at runtime
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
