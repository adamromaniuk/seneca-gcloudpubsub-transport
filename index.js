var gcloud = require('@google-cloud/pubsub');
var _ = require('lodash');
var nid = require('nid');

module.exports = function (options) {
  var seneca = this;
  var plugin = 'gcloudpubsub-transport';
  var pubsub;

  var so = seneca.options();
  var tu = seneca.export('transport/utils');

  options = seneca.util.deepextend({
    gcloud: {
      topicPrefix: '',
      projectId: '',
    }
  }, options);

  var topicPrefix = options.gcloud.topicPrefix;

  seneca.add('role:transport,hook:listen,type:gcloud', hook_listen_gcloud);
  seneca.add('role:transport,hook:client,type:gcloud', hook_client_gcloud);
  seneca.add('role:seneca,cmd:close', shutdown);

  function make_error_handler(type, tag) {
    return function (note, err) {
      seneca.log.error(type, tag, note, err, 'CLOSED');
    }
  }

  function init(opts) {
    return new Promise(function (fulfill, reject) {
      try {
        pubsub = new gcloud({ projectId: opts.projectId });
        topicPrefix = opts.topicPrefix;
        seneca.log.info('Connected to GCloud PubSub');
        fulfill(pubsub);
      }
      catch (ex) {
        reject(ex);
      }
    });
  }

  function createTopics(pubsub) {
    function validatePrefix(topicPrefix) {
      return new Promise(function (fulfill, reject) {
        if (!_.isString(topicPrefix) || _.isEmpty(topicPrefix)
          || topicPrefix.length > 250) {
          reject('topicPrefix must be a valid string 250 characters or less!');
        }
        else {
          fulfill();
        }
      });
    }

    // Create the request and response topics
    function topicCreator(topicName) {
      return new Promise((resolve, reject) => {
        pubsub
          .createTopic(topicName)
          .then(results => {
            const topic = results[0];
            seneca.log.info('Topic "' + topicName + '" created: ', topic);
            return topic;
          })
          .catch(err => {
            if (err.code === 6) {
              seneca.log.warn('Topic "' + topicName + '" already exists.');
              resolve(pubsub.topic(topicName));
            }
            else {
              seneca.log.info('Failed to create topic: ', topicName);
              reject(err);
            }
          });
      });
    }

    return Promise.all([
      validatePrefix(topicPrefix),
      topicCreator(topicPrefix + '.act'),
      topicCreator(topicPrefix + '.res')
    ]).then(function (results) {
      return Promise.resolve({
        act: results[1],
        res: results[2]
      });
    });
  }

  // Subscribe to a topic object
  function createSubscription(topic, kind) {
    return new Promise(function (fulfill, reject) {
      var subscriber_name = topicPrefix + '.' + kind;

      pubsub
        .topic(topic.name)
        .createSubscription(subscriber_name)
        .then(results => {
          const subscription = results[0];
          seneca.log.info('Created subscription to "' + topic.name + '", Subscription: ' + subscriber_name);
          fulfill(subscription);
        })
        .catch(err => {
          seneca.log.error('Failed to subscribe to "' + topic.name + '"');
          reject(err);
        });
    });
  }

  function hook_listen_gcloud(args, done) {
    var type = args.type;
    var listen_options = seneca.util.clean(_.extend({}, options[type], args));
    topicPrefix = listen_options.topicPrefix;

    init(listen_options)
      .then(createTopics)
      .then(subscribeTopics)
      .then(function () {
        done();
      })
      .catch(function (err) {
        done(err);
      });

    function subscribeTopics(topics) {
      var act_topic = topics.act; // The request topic
      var res_topic = topics.res; // The response topic

      return createSubscription(act_topic, 'act')
        .then(attachHandler);

      function attachHandler(subscription) {
        return new Promise(function (fulfill, reject) {
          seneca.log.info('Subscribing to ' + subscription.name);
          subscription.on('message', onMessage);
          fulfill();
        });

        function onMessage(message) {
          var content = Buffer.from(message.data.toString('utf-8'));
          var data = tu.parseJSON(seneca, 'listen-' + type, content);

          // Publish message
          tu.handle_request(seneca, data, listen_options, function (out) {
            if (out == null) return;

            const data = tu.stringifyJSON(seneca, 'listen-' + type, out);
            const dataBuffer = Buffer.from(data);

            pubsub.topic(res_topic.name)
              .publisher()
              .publish(dataBuffer)
              .then(results => {
                message.ack();
              }).catch(err => {
                seneca.log.error('Failed to send message: ' + err);
              });
          });
        }
      }
    }
  }

  function hook_client_gcloud(args, client_done) {
    var seneca = this;
    var type = args.type;
    var client_options = seneca.util.clean(_.extend({}, options[type], args));

    init(client_options)
      .then(createTopics)
      .then(subscribeTopics)
      .then(createClient)
      .then(function (client) {
        client_done(null, client);
      })
      .catch(function (err) {
        client_done(err);
      });

    function subscribeTopics(topics) {
      var res_topic = topics.res; // The response topic

      return createSubscription(res_topic, 'res')
        .then(function (subscription) {
          return Promise.resolve({
            topics: topics,
            subscription: subscription
          });
        });
    }

    function createClient(params) {
      return new Promise(function (fulfill, reject) {
        var act_topic = params.topics.act;
        var subscription = params.subscription;

        // Subscribe to the response topic
        seneca.log.info('Subscribing to ' + subscription.name);
        subscription.on('message', onMessage);

        function onMessage(message) {
          var content = Buffer.from(message.data.toString('utf-8'));
          var input = tu.parseJSON(seneca, 'client-' + type, content);

          console.log("### message: " + JSON.stringify(message));
          console.log("### content: " + content);
          console.log("### input: " + input);

          message.ack();
          console.log('Ack send for message: ' + message.id);
          tu.handle_response(seneca, input, client_options);
        }

        var client = {
          id: nid(),
          toString: function () {
            return 'any-' + this.id;
          },

          // TODO: is this used?
          match: function (args) {
            return !this.has(args);
          },

          send: function (args, done, meta) {
            var outmsg = tu.prepare_request(this, args, done, meta);
            const data = tu.stringifyJSON(seneca, 'client-' + type, outmsg);
            const dataBuffer = Buffer.from(data);
            pubsub.topic(act_topic.name)
              .publisher()
              .publish(dataBuffer)
              .then(results => {
                seneca.log.debug('Message sent successfully: ' + results[0]);
              }).catch(err => {
                seneca.log.error('Failed to send message: ' + err);
              });
          }
        };

        fulfill(client);
      });
    }
  }

  function shutdown(args, done) {
    done();
  }

  return {
    name: plugin
  };
};