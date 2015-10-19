# encoding: utf-8
module Moped
  # Provides the shared behaviour for retry failed operations.
  #
  # @since 2.0.0
  module Retryable

    private

    # Execute the provided block on the cluster and retry if the execution
    # fails.
    #
    # @api private
    #
    # @example Execute with retry.
    #   preference.with_retry(cluster) do
    #     cluster.with_primary do |node|
    #       node.refresh
    #     end
    #   end
    #
    # @param [ Cluster ] cluster The cluster.
    # @param [ Integer ] retries The number of times to retry.
    #
    # @return [ Object ] The result of the block.
    #
    # @since 2.0.0
    def with_retry(cluster, retries = cluster.max_retries, &block)
      begin
        block.call
      rescue Errors::ConnectionFailure, Errors::PotentialReconfiguration, Errors::OperationFailure => e

        raise e if e.instance_of?(Errors::PotentialReconfiguration) &&
          ! (e.message.include?("not master") || e.message.include?("Not primary"))

        # Monkey patch for https://jira.mongodb.org/browse/SERVER-20829
        if e.is_a?(Errors::OperationFailure)
          if e.message.include?("RUNNER_DEAD")
            Loggable.warn("  MOPED:", "[jontest] got RUNNER_DEAD on #{cluster.nodes.inspect}, retries is #{retries}", "n/a")
          else
            raise e
          end
        end

        if retries > 0
          Loggable.info("  MOPED:", "Retrying connection attempt #{retries} more time(s), nodes is #{cluster.nodes.inspect}, seeds are #{cluster.seeds.inspect}, cluster is #{cluster.inspect}. Error backtrace is #{e.backtrace}.", "n/a")
          sleep(cluster.retry_interval)
          cluster.refresh
          with_retry(cluster, retries - 1, &block)
        else
          raise e
        end
      end
    end
  end
end
