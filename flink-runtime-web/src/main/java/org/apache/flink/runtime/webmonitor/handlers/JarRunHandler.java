/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.application.ApplicationRunner;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.handlers.utils.JarHandlerUtils.JarHandlerContext;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.fromRequestBodyOrQueryParameter;
import static org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils.getQueryParameter;
import static org.apache.flink.shaded.guava33.com.google.common.base.Strings.emptyToNull;

/** Handler to submit jobs uploaded via the Web UI. */
public class JarRunHandler
        extends AbstractRestHandler<
                DispatcherGateway, JarRunRequestBody, JarRunResponseBody, JarRunMessageParameters> {

    private final Path jarDir;

    private final Configuration configuration;

    private final ApplicationRunner applicationRunner;

    private final Executor executor;

    public JarRunHandler(
            final GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            final Duration timeout,
            final Map<String, String> responseHeaders,
            final MessageHeaders<JarRunRequestBody, JarRunResponseBody, JarRunMessageParameters>
                    messageHeaders,
            final Path jarDir,
            final Configuration configuration,
            final Executor executor,
            final Supplier<ApplicationRunner> applicationRunnerSupplier) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        this.jarDir = requireNonNull(jarDir);
        this.configuration = requireNonNull(configuration);
        this.executor = requireNonNull(executor);

        this.applicationRunner = applicationRunnerSupplier.get();
    }

    @Override
    @VisibleForTesting
    public CompletableFuture<JarRunResponseBody> handleRequest(
            @Nonnull final HandlerRequest<JarRunRequestBody> request,
            @Nonnull final DispatcherGateway gateway)
            throws RestHandlerException {

        final Configuration effectiveConfiguration = new Configuration(configuration);
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, false);
        effectiveConfiguration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);

        final JarHandlerContext context = JarHandlerContext.fromRequest(request, jarDir, log);
        context.applyToConfiguration(effectiveConfiguration, request);
        SavepointRestoreSettings.toConfiguration(
                getSavepointRestoreSettings(request, effectiveConfiguration),
                effectiveConfiguration);

        final PackagedProgram program = context.toPackagedProgram(effectiveConfiguration);

        return CompletableFuture.supplyAsync(
                        () -> applicationRunner.run(gateway, program, effectiveConfiguration),
                        executor)
                .handle(
                        (jobIds, throwable) -> {
                            program.close();
                            if (throwable != null) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Could not execute application.",
                                                HttpResponseStatus.BAD_REQUEST,
                                                throwable));
                            } else if (jobIds.isEmpty()) {
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "No jobs included in application.",
                                                HttpResponseStatus.BAD_REQUEST));
                            }
                            return new JarRunResponseBody(jobIds.get(0));
                        });
    }

    private SavepointRestoreSettings getSavepointRestoreSettings(
            final @Nonnull HandlerRequest<JarRunRequestBody> request,
            final Configuration effectiveConfiguration)
            throws RestHandlerException {

        final JarRunRequestBody requestBody = request.getRequestBody();

        final boolean allowNonRestoredState =
                fromRequestBodyOrQueryParameter(
                        requestBody.getAllowNonRestoredState(),
                        () -> getQueryParameter(request, AllowNonRestoredStateQueryParameter.class),
                        effectiveConfiguration.get(
                                StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE),
                        log);
        final String savepointPath =
                fromRequestBodyOrQueryParameter(
                        emptyToNull(requestBody.getSavepointPath()),
                        () ->
                                emptyToNull(
                                        getQueryParameter(
                                                request, SavepointPathQueryParameter.class)),
                        effectiveConfiguration.get(StateRecoveryOptions.SAVEPOINT_PATH),
                        log);
        final RecoveryClaimMode recoveryClaimMode =
                Optional.ofNullable(requestBody.getRecoveryClaimMode())
                        .orElseGet(
                                () ->
                                        effectiveConfiguration.get(
                                                StateRecoveryOptions.RESTORE_MODE));
        if (requestBody.isDeprecatedRestoreModeHasValue()) {
            log.warn(
                    "The option 'restoreMode' is deprecated, please use 'recoveryClaimMode' instead.");
        }
        if (recoveryClaimMode.equals(RecoveryClaimMode.LEGACY)) {
            log.warn(
                    "The {} restore mode is deprecated, please use {} or {} mode instead.",
                    RecoveryClaimMode.LEGACY,
                    RecoveryClaimMode.CLAIM,
                    RecoveryClaimMode.NO_CLAIM);
        }
        final SavepointRestoreSettings savepointRestoreSettings;
        if (savepointPath != null) {
            savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(
                            savepointPath, allowNonRestoredState, recoveryClaimMode);
        } else {
            savepointRestoreSettings = SavepointRestoreSettings.none();
        }
        return savepointRestoreSettings;
    }
}
