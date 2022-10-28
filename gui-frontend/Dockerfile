FROM node:16

# Create app directory
WORKDIR /app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY gui-frontend/package*.json ./


RUN npm install
# If you are building your code for production
# RUN npm ci --only=production

COPY gui-frontend/material-icons/fonts/ /app/node_modules/svelte-material-ui/fonts/
COPY gui-frontend/material-icons/icons.css /app/node_modules/svelte-material-ui/

# Bundle app source
COPY gui-frontend/ /app/

EXPOSE 5173
CMD [ "npm", "run", "dev" ]