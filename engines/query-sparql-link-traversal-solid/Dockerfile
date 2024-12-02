FROM node:18

# Install location
ENV dir /var/www/@comunica/query-sparql-link-traversal-solid/

# Copy the engine files (generated from package.json!files)
COPY config ${dir}/config/
COPY lib/*.js ${dir}/lib/
COPY bin/*.js ${dir}/bin/
COPY engine-default.js package.json ${dir}

# Set the npm registry
ARG NPM_REGISTRY=https://registry.npmjs.org/

# OWN CODE
RUN echo ${NPM_REGISTRY}

# Check if Verdaccio registry is available and display the result
RUN curl --silent --head http://host.docker.internal:4873/ || \
    (echo "Verdaccio registry is unavailable" && exit 1)

RUN curl --silent --head http://host.docker.internal:4873/@comunica%2factor-bindings-aggregator-factory-average || \
(echo "Verdaccio registry is unavailable" && exit 1)

RUN npm config set @comunica:registry $NPM_REGISTRY

# Test installing a package
RUN npm install -g @comunica/actor-bindings-aggregator-factory-average --verbose || \
    (echo "Package installation failed" && exit 1)

# Install the node module
RUN cd ${dir} && npm install --only=production --verbose --ignore-engines

# Run base binary (generated from package.json!bin)
WORKDIR ${dir}
ENTRYPOINT ["node", "./bin/query.js"]

# Default command
CMD ["--help"]
